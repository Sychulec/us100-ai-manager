import os
import re
import time
import math
import uuid
import json
import tempfile
import threading
import unicodedata
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, redirect, request
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
    ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
    ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
    ProtoOAAccountDisconnectEvent, ProtoOAAccountsTokenInvalidatedEvent,
    ProtoOATraderReq, ProtoOATraderRes,
    ProtoOAReconcileReq, ProtoOAReconcileRes,
    ProtoOASymbolsListReq, ProtoOASymbolsListRes,
    ProtoOASymbolByIdReq, ProtoOASymbolByIdRes,
    ProtoOAGetTrendbarsReq, ProtoOAGetTrendbarsRes,
    ProtoOAGetPositionUnrealizedPnLReq, ProtoOAGetPositionUnrealizedPnLRes,
    ProtoOANewOrderReq, ProtoOAExecutionEvent,
    ProtoOAOrderErrorEvent, ProtoOAErrorRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOATrendbarPeriod, ProtoOAOrderType, ProtoOATradeSide,
    ProtoOAExecutionType,
)
from twisted.internet import reactor

app = Flask(__name__)

TARGET_TRADER_LOGIN = 5901967
FIXED_LOTS_US100 = float(os.environ.get("FIXED_LOTS_US100", "1.50"))
ALLOWED_INSTRUMENTS = ("US100",)
BOT_LABEL = "US100_V9_DEMO"

CTRADER_CLIENT_ID = os.environ.get("CTRADER_CLIENT_ID")
CTRADER_CLIENT_SECRET = os.environ.get("CTRADER_CLIENT_SECRET")
CTRADER_REDIRECT_URI = os.environ.get("CTRADER_REDIRECT_URI")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
AUTO_REFRESH_SECONDS = int(os.environ.get("AUTO_REFRESH_SECONDS", "300"))

def choose_state_dir():
    configured = os.environ.get("BOT_STATE_DIR")
    candidates = [configured] if configured else ["/var/data", "/tmp/us100-v9"]
    for path in candidates:
        if not path:
            continue
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, ".write_test")
            with open(probe, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(probe)
            return path
        except Exception:
            pass
    return tempfile.gettempdir()

BOT_STATE_DIR = choose_state_dir()
BOT_STATE_FILE = os.path.join(BOT_STATE_DIR, "us100_v9_state.json")

ctrader_state = {
    "access_token": None, "refresh_token": None,
    "connected": False, "application_authorized": False,
    "account_authorized": False, "account_id": None,
    "balance": None, "equity": None, "unrealized_pnl": 0.0,
    "positions": [], "orders": [], "market_ready": False,
    "last_market_refresh": None, "error": None,
}
trade_state = {
    "pending_symbols": set(), "last_signal_key": {},
    "last_order": None, "last_order_error": None,
}
market_state = {
    "US100": {
        "found": False, "symbol_id": None, "symbol_name": None,
        "digits": None, "min_volume_raw": None, "max_volume_raw": None,
        "step_volume_raw": None, "lot_size_raw": None,
        "candles": {"M15": [], "H1": []},
    }
}
ctrader_client = None
reactor_started = False
refresh_started = False
state_lock = threading.Lock()

print(f"[BOOT] US100 V9 DEMO | target={TARGET_TRADER_LOGIN} | lots={FIXED_LOTS_US100:.2f}", flush=True)

def set_error(error):
    ctrader_state["error"] = str(error)
    print("[ERROR]", error, flush=True)

def clear_error():
    ctrader_state["error"] = None

def safe_errback(failure):
    set_error(str(failure))

def normalize_symbol(name):
    return re.sub(r"[^A-Z0-9]", "", str(name or "").upper())

def detect_instrument(name):
    value = normalize_symbol(name)
    if any(x in value for x in ("US100", "NAS100", "USTEC", "USTECH100", "NASDAQ100")):
        return "US100"
    return None

def strip_accents(value):
    value = unicodedata.normalize("NFKD", str(value))
    return "".join(ch for ch in value if not unicodedata.combining(ch))

def send_telegram_message(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM DISABLED]", text, flush=True)
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=15,
        )
        response.raise_for_status()
        return True
    except Exception as error:
        print("[TELEGRAM ERROR]", error, flush=True)
        return False

def save_state():
    payload = {
        "access_token": ctrader_state.get("access_token"),
        "refresh_token": ctrader_state.get("refresh_token"),
        "last_signal_key": trade_state.get("last_signal_key", {}),
    }
    temp = BOT_STATE_FILE + ".tmp"
    try:
        with state_lock:
            with open(temp, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
            os.replace(temp, BOT_STATE_FILE)
        return True
    except Exception as error:
        print("[PERSISTENCE ERROR]", error, flush=True)
        return False

def load_state():
    if not os.path.exists(BOT_STATE_FILE):
        return False
    try:
        with state_lock:
            with open(BOT_STATE_FILE, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        ctrader_state["access_token"] = payload.get("access_token")
        ctrader_state["refresh_token"] = payload.get("refresh_token")
        trade_state["last_signal_key"] = dict(payload.get("last_signal_key", {}) or {})
        return True
    except Exception as error:
        print("[PERSISTENCE ERROR]", error, flush=True)
        return False

def extract_number(label, text):
    match = re.search(rf"{label}\s*:\s*(-?[0-9]+(?:[.,][0-9]+)?)", text, re.I)
    return float(match.group(1).replace(",", ".")) if match else None

def parse_strategy_alert(text):
    raw = str(text or "").strip()
    normalized = re.sub(r"\s+", " ", strip_accents(raw).upper()).strip()

    if "WEJSCIE LONG" in normalized:
        side = "LONG"
    elif "WEJSCIE SHORT" in normalized:
        side = "SHORT"
    else:
        side = None

    symbol_match = re.search(r"SYMBOL\s*:\s*([^|]+)", normalized, re.I)
    tf_match = re.search(r"TF\s*:\s*([^|]+)", normalized, re.I)
    symbol_raw = symbol_match.group(1).strip() if symbol_match else ""
    timeframe = tf_match.group(1).strip().lower() if tf_match else "?"

    generic_fill = bool(
        re.search(r"\bORDER\s+(BUY|SELL)\b.*\bFILLED\s+ON\s+US100\b", normalized)
    )

    return {
        "event": "ENTRY" if side else ("GENERIC_FILL" if generic_fill else "UNKNOWN"),
        "side": side,
        "symbol": detect_instrument(symbol_raw),
        "timeframe": timeframe,
        "strategy_entry": extract_number("CENA", normalized),
        "strategy_sl": extract_number("SL", normalized),
        "strategy_tp": extract_number("TP", normalized),
        "raw": raw,
    }

def fixed_volume_for_lots(lots):
    data = market_state["US100"]
    lot_size = int(data.get("lot_size_raw") or 0)
    min_volume = int(data.get("min_volume_raw") or 0)
    max_volume = int(data.get("max_volume_raw") or 0)
    step_volume = int(data.get("step_volume_raw") or 0)
    if min(lot_size, min_volume, max_volume, step_volume) <= 0:
        return None
    desired = float(lots) * lot_size
    steps = round(desired / step_volume)
    raw = int(steps * step_volume)
    if abs(raw - desired) > 1e-6:
        return None
    if not (min_volume <= raw <= max_volume):
        return None
    return raw

def open_position_symbol_ids():
    return {
        int(position["symbol_id"])
        for position in ctrader_state["positions"]
        if position.get("symbol_id") is not None
    }

def submit_strategy_market_order(signal):
    if ctrader_client is None or not ctrader_state["account_authorized"]:
        print("[TV] BLOCKED: cTrader not authorized", flush=True)
        return

    if "US100" in trade_state["pending_symbols"]:
        print("[TV] BLOCKED: order already pending", flush=True)
        return

    symbol_id = market_state["US100"]["symbol_id"]
    if not symbol_id:
        print("[TV] BLOCKED: symbol not ready", flush=True)
        return
    if int(symbol_id) in open_position_symbol_ids():
        print("[TV] BLOCKED: US100 position already open", flush=True)
        return

    entry = signal["strategy_entry"]
    sl = signal["strategy_sl"]
    tp = signal["strategy_tp"]

    if signal["side"] == "LONG" and not (sl < entry < tp):
        print("[TV] BLOCKED: invalid LONG SL/TP", flush=True)
        return
    if signal["side"] == "SHORT" and not (tp < entry < sl):
        print("[TV] BLOCKED: invalid SHORT SL/TP", flush=True)
        return

    signal_key = f"{signal['side']}:{signal['timeframe']}:{entry:.5f}:{sl:.5f}:{tp:.5f}"
    if trade_state["last_signal_key"].get("US100") == signal_key:
        print("[TV] BLOCKED: duplicate signal", flush=True)
        return

    volume_raw = fixed_volume_for_lots(FIXED_LOTS_US100)
    if volume_raw is None:
        print("[TV] BLOCKED: exact 1.50 lot volume unavailable", flush=True)
        send_telegram_message("â ď¸ US100: nie mogÄ ustawiÄ dokĹadnie 1.50 lot.")
        return

    req = ProtoOANewOrderReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.symbolId = int(symbol_id)
    req.orderType = ProtoOAOrderType.MARKET
    req.tradeSide = ProtoOATradeSide.BUY if signal["side"] == "LONG" else ProtoOATradeSide.SELL
    req.volume = int(volume_raw)
    req.relativeStopLoss = max(1, int(round(abs(entry - sl) * 100000.0)))
    req.relativeTakeProfit = max(1, int(round(abs(tp - entry) * 100000.0)))
    req.label = BOT_LABEL
    req.comment = f"TV US100 M15 {signal['side']} {FIXED_LOTS_US100:.2f} lot"[:512]
    req.clientOrderId = f"V9-{int(time.time())}-{uuid.uuid4().hex[:8]}"[:50]

    trade_state["pending_symbols"].add("US100")
    trade_state["last_signal_key"]["US100"] = signal_key
    trade_state["last_order"] = {
        "side": signal["side"], "entry": entry, "sl": sl, "tp": tp,
        "lots": FIXED_LOTS_US100, "volume_raw": volume_raw,
        "status": "SENT", "sent_at": int(time.time()),
    }
    save_state()

    print("[ORDER SEND]", trade_state["last_order"], flush=True)
    send_telegram_message(
        f"đĽ US100 {signal['side']} M15\n"
        f"Cena: {entry:.2f}\nSL: {sl:.2f}\nTP: {tp:.2f}\n"
        f"Wolumen: {FIXED_LOTS_US100:.2f} lot\nâĄď¸ cTrader DEMO"
    )

    def order_errback(failure):
        trade_state["pending_symbols"].discard("US100")
        trade_state["last_order_error"] = str(failure)
        set_error(failure)

    ctrader_client.send(req).addErrback(order_errback)

def process_strategy_alert(text):
    print("[TV] RAW ALERT:", str(text).replace("\n", " | ")[:1000], flush=True)
    signal = parse_strategy_alert(text)
    print(
        f"[TV] PARSED event={signal['event']} side={signal['side']} "
        f"symbol={signal['symbol']} tf={signal['timeframe']} "
        f"entry={signal['strategy_entry']} sl={signal['strategy_sl']} tp={signal['strategy_tp']}",
        flush=True,
    )

    if signal["event"] == "GENERIC_FILL":
        print("[TV] REJECTED: generic TradingView fill lacks strategy Cena/SL/TP", flush=True)
        return
    if signal["event"] != "ENTRY" or signal["side"] not in ("LONG", "SHORT"):
        print("[TV] REJECTED: no strategy entry", flush=True)
        return
    if signal["symbol"] != "US100":
        print("[TV] REJECTED: symbol", flush=True)
        return
    if signal["timeframe"] not in ("15", "15m", "15min", "m15"):
        print("[TV] REJECTED: timeframe", flush=True)
        return
    if None in (signal["strategy_entry"], signal["strategy_sl"], signal["strategy_tp"]):
        print("[TV] REJECTED: missing Cena/SL/TP", flush=True)
        return
    if not getattr(reactor, "running", False):
        print("[TV] REJECTED: reactor not running", flush=True)
        return

    print("[TV] ACCEPTED -> CTRADER DEMO", flush=True)
    reactor.callFromThread(submit_strategy_market_order, signal)

def trendbar_to_dict(bar, digits):
    low = int(bar.low)
    return {
        "timestamp": int(bar.utcTimestampInMinutes) * 60,
        "open": round((low + int(bar.deltaOpen)) / 100000.0, digits),
        "high": round((low + int(bar.deltaHigh)) / 100000.0, digits),
        "low": round(low / 100000.0, digits),
        "close": round((low + int(bar.deltaClose)) / 100000.0, digits),
        "volume": int(bar.volume),
    }

def request_trendbars(client, timeframe):
    symbol_id = market_state["US100"]["symbol_id"]
    if not ctrader_state["account_authorized"] or not symbol_id:
        return
    now_ms = int(time.time() * 1000)
    req = ProtoOAGetTrendbarsReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.symbolId = int(symbol_id)
    req.period = ProtoOATrendbarPeriod.M15 if timeframe == "M15" else ProtoOATrendbarPeriod.H1
    req.fromTimestamp = now_ms - (14 if timeframe == "M15" else 30) * 86400000
    req.toTimestamp = now_ms
    req.count = 250
    client.send(req).addErrback(safe_errback)

def request_account_data(client):
    if not ctrader_state["account_authorized"]:
        return
    account_id = int(ctrader_state["account_id"])

    if not market_state["US100"]["found"]:
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = account_id
        req.includeArchivedSymbols = False
        client.send(req).addErrback(safe_errback)

    for req in (ProtoOATraderReq(), ProtoOAReconcileReq(), ProtoOAGetPositionUnrealizedPnLReq()):
        req.ctidTraderAccountId = account_id
        client.send(req).addErrback(safe_errback)

def refresh_market():
    if ctrader_client is None or not ctrader_state["account_authorized"]:
        return
    request_account_data(ctrader_client)
    if market_state["US100"]["digits"] is not None:
        request_trendbars(ctrader_client, "M15")
        request_trendbars(ctrader_client, "H1")

def start_refresh_loop():
    global refresh_started
    if refresh_started:
        return
    refresh_started = True

    def tick():
        refresh_market()
        reactor.callLater(AUTO_REFRESH_SECONDS, tick)

    reactor.callLater(AUTO_REFRESH_SECONDS, tick)

def send_account_list(client):
    token = ctrader_state.get("access_token")
    if not token:
        set_error("Brak access_token")
        return
    req = ProtoOAGetAccountListByAccessTokenReq()
    req.accessToken = token
    client.send(req).addErrback(safe_errback)

def authorize_account(client):
    req = ProtoOAAccountAuthReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.accessToken = ctrader_state["access_token"]
    client.send(req).addErrback(safe_errback)

def start_ctrader_connection():
    global ctrader_client

    if ctrader_client is not None and ctrader_state["connected"]:
        send_account_list(ctrader_client)
        return

    # Hard DEMO endpoint.
    ctrader_client = Client(
        EndPoints.PROTOBUF_DEMO_HOST,
        EndPoints.PROTOBUF_PORT,
        TcpProtocol,
    )

    def connected(client):
        ctrader_state["connected"] = True
        clear_error()
        print("[CTRADER] CONNECTED DEMO", flush=True)
        req = ProtoOAApplicationAuthReq()
        req.clientId = CTRADER_CLIENT_ID
        req.clientSecret = CTRADER_CLIENT_SECRET
        client.send(req).addErrback(safe_errback)

    def disconnected(client, reason):
        ctrader_state["connected"] = False
        ctrader_state["account_authorized"] = False
        print("[CTRADER] DISCONNECTED", reason, flush=True)

    def on_message(client, message):
        payload_type = message.payloadType

        if payload_type == ProtoOAApplicationAuthRes().payloadType:
            ctrader_state["application_authorized"] = True
            send_account_list(client)

        elif payload_type == ProtoOAGetAccountListByAccessTokenRes().payloadType:
            response = Protobuf.extract(message)
            matches = [
                account for account in response.ctidTraderAccount
                if int(getattr(account, "traderLogin", 0) or 0) == TARGET_TRADER_LOGIN
            ]
            if not matches:
                set_error(f"Nie znaleziono traderLogin={TARGET_TRADER_LOGIN}")
                return

            account = matches[0]
            if bool(getattr(account, "isLive", False)):
                ctrader_state["account_id"] = None
                ctrader_state["account_authorized"] = False
                set_error("LIVE BLOCKED - ten bot jest tylko DEMO")
                return

            ctrader_state["account_id"] = int(account.ctidTraderAccountId)
            print(
                f"[CTRADER] DEMO TARGET FOUND login={TARGET_TRADER_LOGIN} "
                f"account_id={ctrader_state['account_id']}",
                flush=True,
            )
            authorize_account(client)

        elif payload_type == ProtoOAAccountAuthRes().payloadType:
            ctrader_state["account_authorized"] = True
            clear_error()
            print("[CTRADER] ACCOUNT AUTHORIZED DEMO", flush=True)
            request_account_data(client)
            start_refresh_loop()

        elif payload_type == ProtoOAAccountDisconnectEvent().payloadType:
            ctrader_state["account_authorized"] = False
            reactor.callLater(1.0, authorize_account, client)

        elif payload_type == ProtoOAAccountsTokenInvalidatedEvent().payloadType:
            ctrader_state["account_authorized"] = False
            set_error("Token cTrader wygasĹ - zaloguj ponownie przez /ctrader/login")

        elif payload_type == ProtoOATraderRes().payloadType:
            response = Protobuf.extract(message)
            trader = response.trader
            divisor = 10 ** trader.moneyDigits
            ctrader_state["balance"] = round(trader.balance / divisor, 2)
            if ctrader_state["equity"] is None:
                ctrader_state["equity"] = ctrader_state["balance"]

        elif payload_type == ProtoOAGetPositionUnrealizedPnLRes().payloadType:
            response = Protobuf.extract(message)
            divisor = 10 ** response.moneyDigits
            pnl = sum(x.netUnrealizedPnL for x in response.positionUnrealizedPnL) / divisor
            ctrader_state["unrealized_pnl"] = round(pnl, 2)
            if ctrader_state["balance"] is not None:
                ctrader_state["equity"] = round(ctrader_state["balance"] + pnl, 2)

        elif payload_type == ProtoOAReconcileRes().payloadType:
            response = Protobuf.extract(message)
            ctrader_state["positions"] = [
                {
                    "position_id": int(position.positionId),
                    "symbol_id": int(position.tradeData.symbolId),
                    "volume_raw": int(position.tradeData.volume),
                    "side": int(position.tradeData.tradeSide),
                    "price": float(getattr(position, "price", 0.0) or 0.0),
                    "stop_loss": float(getattr(position, "stopLoss", 0.0) or 0.0),
                    "take_profit": float(getattr(position, "takeProfit", 0.0) or 0.0),
                }
                for position in response.position
            ]
            ctrader_state["orders"] = [int(order.orderId) for order in response.order]

        elif payload_type == ProtoOASymbolsListRes().payloadType:
            response = Protobuf.extract(message)
            candidates = [
                symbol for symbol in response.symbol
                if detect_instrument(symbol.symbolName) == "US100"
            ]
            if not candidates:
                set_error("Nie znaleziono US100 / US TECH 100")
                return

            exact = [s for s in candidates if normalize_symbol(s.symbolName) == "US100"]
            symbol = exact[0] if exact else candidates[0]
            data = market_state["US100"]
            data["found"] = True
            data["symbol_id"] = int(symbol.symbolId)
            data["symbol_name"] = symbol.symbolName

            req = ProtoOASymbolByIdReq()
            req.ctidTraderAccountId = int(ctrader_state["account_id"])
            req.symbolId.append(int(symbol.symbolId))
            client.send(req).addErrback(safe_errback)

        elif payload_type == ProtoOASymbolByIdRes().payloadType:
            response = Protobuf.extract(message)
            for symbol in response.symbol:
                if int(symbol.symbolId) != int(market_state["US100"]["symbol_id"]):
                    continue
                data = market_state["US100"]
                data["digits"] = int(symbol.digits)
                data["min_volume_raw"] = int(getattr(symbol, "minVolume", 0) or 0)
                data["max_volume_raw"] = int(getattr(symbol, "maxVolume", 0) or 0)
                data["step_volume_raw"] = int(getattr(symbol, "stepVolume", 0) or 0)
                data["lot_size_raw"] = int(getattr(symbol, "lotSize", 0) or 0)
                print(
                    f"[CTRADER] SYMBOL READY {data['symbol_name']} "
                    f"lotSize={data['lot_size_raw']} min={data['min_volume_raw']} "
                    f"max={data['max_volume_raw']} step={data['step_volume_raw']}",
                    flush=True,
                )
                request_trendbars(client, "M15")
                request_trendbars(client, "H1")

        elif payload_type == ProtoOAGetTrendbarsRes().payloadType:
            response = Protobuf.extract(message)
            if int(response.symbolId) != int(market_state["US100"]["symbol_id"]):
                return
            timeframe = (
                "M15" if response.period == ProtoOATrendbarPeriod.M15
                else "H1" if response.period == ProtoOATrendbarPeriod.H1
                else None
            )
            if timeframe is None:
                return
            digits = market_state["US100"]["digits"] or 2
            candles = [trendbar_to_dict(bar, digits) for bar in response.trendbar]
            candles.sort(key=lambda item: item["timestamp"])
            market_state["US100"]["candles"][timeframe] = candles
            ctrader_state["market_ready"] = len(market_state["US100"]["candles"]["M15"]) > 0
            ctrader_state["last_market_refresh"] = int(time.time())

        elif payload_type == ProtoOAExecutionEvent().payloadType:
            response = Protobuf.extract(message)
            symbol_id = None
            if response.HasField("order"):
                symbol_id = int(response.order.tradeData.symbolId)
            elif response.HasField("position"):
                symbol_id = int(response.position.tradeData.symbolId)

            if symbol_id == market_state["US100"]["symbol_id"]:
                trade_state["pending_symbols"].discard("US100")

            if int(response.executionType) == ProtoOAExecutionType.ORDER_FILLED:
                last = trade_state.get("last_order") or {}
                if last.get("status") == "SENT":
                    last["status"] = "FILLED"
                    save_state()
                    print("[ORDER FILLED] US100", flush=True)
                    send_telegram_message(
                        "â US100: pozycja zostaĹa otwarta na cTrader DEMO."
                    )
                reactor.callLater(0.5, request_account_data, client)

            elif int(response.executionType) == ProtoOAExecutionType.ORDER_REJECTED:
                if trade_state.get("last_order"):
                    trade_state["last_order"]["status"] = "REJECTED"
                trade_state["last_order_error"] = str(
                    getattr(response, "errorCode", "ORDER_REJECTED")
                )

        elif payload_type == ProtoOAOrderErrorEvent().payloadType:
            response = Protobuf.extract(message)
            trade_state["pending_symbols"].clear()
            trade_state["last_order_error"] = {
                "code": str(response.errorCode),
                "description": str(response.description),
            }
            set_error(f"{response.errorCode}: {response.description}")

        elif payload_type == ProtoOAErrorRes().payloadType:
            response = Protobuf.extract(message)
            code = str(response.errorCode)
            description = str(response.description)
            if "ALREADY_LOGGED_IN" in code or "ALREADY_LOGGED_IN" in description:
                if ctrader_state.get("account_id"):
                    ctrader_state["account_authorized"] = True
                    request_account_data(client)
                else:
                    ctrader_state["application_authorized"] = True
                    send_account_list(client)
                return
            set_error(f"{code}: {description}")

    ctrader_client.setConnectedCallback(connected)
    ctrader_client.setDisconnectedCallback(disconnected)
    ctrader_client.setMessageReceivedCallback(on_message)
    ctrader_client.startService()

def start_reactor():
    global reactor_started
    if reactor_started:
        return
    reactor_started = True
    reactor.run(installSignalHandlers=False)

def bootstrap():
    load_state()
    if not ctrader_state.get("access_token"):
        print("[BOOT] Login required: /ctrader/login", flush=True)
        return
    threading.Thread(target=start_reactor, daemon=True).start()
    for _ in range(50):
        if getattr(reactor, "running", False):
            break
        time.sleep(0.1)
    if getattr(reactor, "running", False):
        reactor.callFromThread(start_ctrader_connection)

@app.route("/")
def home():
    return jsonify({
        "bot": "US100 V9 DEMO",
        "status": "ONLINE",
        "demo_only": True,
        "target_trader_login": TARGET_TRADER_LOGIN,
        "fixed_lots_us100": FIXED_LOTS_US100,
        "connected": ctrader_state["connected"],
        "account_authorized": ctrader_state["account_authorized"],
        "market_ready": ctrader_state["market_ready"],
        "balance": ctrader_state["balance"],
        "equity": ctrader_state["equity"],
        "last_order": trade_state["last_order"],
        "error": ctrader_state["error"],
        "state_dir": BOT_STATE_DIR,
    })

@app.route("/health")
def health():
    return jsonify({"status": "ok", "demo_only": True})

@app.route("/ctrader/status")
def ctrader_status():
    data = market_state["US100"]
    return jsonify({
        "target_trader_login": TARGET_TRADER_LOGIN,
        "ctid_trader_account_id": ctrader_state["account_id"],
        "route": "DEMO",
        "connected": ctrader_state["connected"],
        "application_authorized": ctrader_state["application_authorized"],
        "account_authorized": ctrader_state["account_authorized"],
        "market_ready": ctrader_state["market_ready"],
        "balance": ctrader_state["balance"],
        "fixed_lots_us100": FIXED_LOTS_US100,
        "symbol": data["symbol_name"],
        "lot_size_raw": data["lot_size_raw"],
        "min_volume_raw": data["min_volume_raw"],
        "max_volume_raw": data["max_volume_raw"],
        "step_volume_raw": data["step_volume_raw"],
        "last_error": ctrader_state["error"],
    })

@app.route("/trade/status")
def trade_status():
    return jsonify({
        "pending_symbols": list(trade_state["pending_symbols"]),
        "positions": ctrader_state["positions"],
        "last_order": trade_state["last_order"],
        "last_order_error": trade_state["last_order_error"],
    })

@app.route("/telegram/test")
def telegram_test():
    ok = send_telegram_message(
        "đ§Ş US100 V9 DEMO\nTelegram dziaĹa poprawnie."
    )
    return jsonify({"status": "success" if ok else "error", "telegram_sent": ok})

@app.route("/webhook", methods=["POST"])
def webhook():
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"status": "error", "message": "invalid secret"}), 403

    if request.is_json:
        data = request.get_json(silent=True)
        if isinstance(data, dict):
            text = data.get("message") or data.get("text") or json.dumps(data)
        else:
            text = str(data or "")
    else:
        text = request.get_data(as_text=True)

    if not text or not text.strip():
        return jsonify({"status": "error", "message": "empty alert"}), 400

    print(
        f"[WEBHOOK] RECEIVED content_type={request.content_type} "
        f"bytes={len(text.encode('utf-8', errors='ignore'))}",
        flush=True,
    )
    threading.Thread(target=process_strategy_alert, args=(text,), daemon=True).start()
    return jsonify({"status": "accepted", "mode": "US100_V9_DEMO"}), 200

@app.route("/market/refresh")
def market_refresh():
    if ctrader_client is None or not ctrader_state["account_authorized"]:
        return jsonify({"status": "error", "message": "cTrader not authorized"}), 503
    reactor.callFromThread(refresh_market)
    return jsonify({"status": "refresh_started"})

@app.route("/ctrader/login")
def ctrader_login():
    return redirect(
        "https://id.ctrader.com/my/settings/openapi/grantingaccess/?"
        + urlencode({
            "client_id": CTRADER_CLIENT_ID,
            "redirect_uri": CTRADER_REDIRECT_URI,
            "scope": "trading",
            "product": "web",
        })
    )

@app.route("/ctrader/callback")
def ctrader_callback():
    code = request.args.get("code")
    if not code:
        return jsonify({"status": "error", "message": "Brak kodu OAuth"}), 400
    try:
        response = requests.get(
            "https://openapi.ctrader.com/apps/token",
            params={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": CTRADER_REDIRECT_URI,
                "client_id": CTRADER_CLIENT_ID,
                "client_secret": CTRADER_CLIENT_SECRET,
            },
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        ctrader_state["access_token"] = data.get("accessToken")
        ctrader_state["refresh_token"] = data.get("refreshToken")
        save_state()
    except Exception as error:
        set_error(error)
        return jsonify({"status": "error", "message": "Token error"}), 500

    if not reactor_started:
        threading.Thread(target=start_reactor, daemon=True).start()
        time.sleep(0.5)
    reactor.callFromThread(start_ctrader_connection)
    return jsonify({
        "status": "success",
        "route": "DEMO",
        "target_trader_login": TARGET_TRADER_LOGIN,
    })

threading.Thread(target=bootstrap, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
