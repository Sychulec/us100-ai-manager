# US100 V10 DEMO
# PeĹna wersja: V9 entry + awaryjny SL 0.70% + brak twardego TP
# + Trade Manager tunelu regresji M15 -> H1
# DEMO ONLY: traderLogin 5901967

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
    ProtoOAAmendPositionSLTPReq, ProtoOAClosePositionReq,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOATrendbarPeriod, ProtoOAOrderType, ProtoOATradeSide,
    ProtoOAExecutionType,
)
from twisted.internet import reactor

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

TARGET_TRADER_LOGIN = 5901967
FIXED_LOTS_US100 = float(os.environ.get("FIXED_LOTS_US100", "2.00"))
BOT_LABEL = "US100_V10_DEMO"

EMERGENCY_SL_PCT = float(os.environ.get("EMERGENCY_SL_PCT", "0.0070"))
REG_LENGTH = int(os.environ.get("REG_LENGTH", "100"))
REG_DEV = float(os.environ.get("REG_DEV", "2.0"))
M15_STRUCTURE_BARS = int(os.environ.get("M15_STRUCTURE_BARS", "4"))
H1_STRUCTURE_BARS = int(os.environ.get("H1_STRUCTURE_BARS", "3"))
AUTO_REFRESH_SECONDS = int(os.environ.get("AUTO_REFRESH_SECONDS", "60"))

CTRADER_CLIENT_ID = os.environ.get("CTRADER_CLIENT_ID")
CTRADER_CLIENT_SECRET = os.environ.get("CTRADER_CLIENT_SECRET")
CTRADER_REDIRECT_URI = os.environ.get("CTRADER_REDIRECT_URI")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

# ============================================================
# STATE DIRECTORY
# ============================================================

def choose_state_dir():
    configured = os.environ.get("BOT_STATE_DIR")
    candidates = [configured] if configured else ["/var/data", "/tmp/us100-v10"]

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
BOT_STATE_FILE = os.path.join(BOT_STATE_DIR, "us100_v10_state.json")

# ============================================================
# GLOBAL STATE
# ============================================================

ctrader_state = {
    "access_token": None,
    "refresh_token": None,
    "connected": False,
    "application_authorized": False,
    "account_authorized": False,
    "account_id": None,
    "balance": None,
    "equity": None,
    "unrealized_pnl": 0.0,
    "positions": [],
    "orders": [],
    "market_ready": False,
    "last_market_refresh": None,
    "error": None,
}

trade_state = {
    "pending_symbols": set(),
    "last_signal_key": {},
    "last_order": None,
    "last_order_error": None,
}

manager_state = {}

market_state = {
    "US100": {
        "found": False,
        "symbol_id": None,
        "symbol_name": None,
        "digits": None,
        "min_volume_raw": None,
        "max_volume_raw": None,
        "step_volume_raw": None,
        "lot_size_raw": None,
        "candles": {"M15": [], "H1": []},
    }
}

ctrader_client = None
reactor_started = False
refresh_started = False
state_lock = threading.Lock()

print(
    f"[BOOT] US100 V10 DEMO | target={TARGET_TRADER_LOGIN} | "
    f"lots={FIXED_LOTS_US100:.2f} | emergencySL={EMERGENCY_SL_PCT*100:.2f}%",
    flush=True,
)

# ============================================================
# HELPERS
# ============================================================

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

# ============================================================
# SAVE / LOAD
# ============================================================

def save_state():
    payload = {
        "access_token": ctrader_state.get("access_token"),
        "refresh_token": ctrader_state.get("refresh_token"),
        "last_signal_key": trade_state.get("last_signal_key", {}),
        "manager_state": manager_state,
    }
    temp = BOT_STATE_FILE + ".tmp"
    try:
        with state_lock:
            with open(temp, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
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
        manager_state.clear()
        manager_state.update(dict(payload.get("manager_state", {}) or {}))
        return True
    except Exception as error:
        print("[PERSISTENCE ERROR]", error, flush=True)
        return False

# ============================================================
# TRADINGVIEW PARSER
# ============================================================

def extract_number(label, text):
    match = re.search(
        rf"{label}\s*:\s*(-?[0-9]+(?:[.,][0-9]+)?)",
        text,
        re.I,
    )
    if not match:
        return None
    return float(match.group(1).replace(",", "."))

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

# ============================================================
# VOLUME
# ============================================================

def fixed_volume_for_lots(lots):
    data = market_state["US100"]

    lot_size = int(data.get("lot_size_raw") or 0)
    min_volume = int(data.get("min_volume_raw") or 0)
    max_volume = int(data.get("max_volume_raw") or 0)
    step_volume = int(data.get("step_volume_raw") or 0)

    if min(lot_size, min_volume, max_volume, step_volume) <= 0:
        return None

    desired = float(lots) * lot_size
    raw = int(round(desired))

    if raw % step_volume != 0:
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

# ============================================================
# REGRESSION CHANNEL
# ============================================================

def closed_candles(timeframe):
    candles = list(market_state["US100"]["candles"].get(timeframe, []))
    if not candles:
        return []

    now = int(time.time())
    seconds = 900 if timeframe == "M15" else 3600

    return [
        c for c in candles
        if int(c["timestamp"]) + seconds <= now
    ]

def regression_channel(candles):
    if len(candles) < REG_LENGTH:
        return None

    window = candles[-REG_LENGTH:]
    y = [float(c["close"]) for c in window]
    n = len(y)

    mean_x = (n - 1) / 2.0
    mean_y = sum(y) / n

    var_x = sum((i - mean_x) ** 2 for i in range(n))
    if var_x <= 0:
        return None

    cov = sum((i - mean_x) * (y[i] - mean_y) for i in range(n))
    slope = cov / var_x
    intercept = mean_y - slope * mean_x

    fitted = [intercept + slope * i for i in range(n)]
    residuals = [y[i] - fitted[i] for i in range(n)]
    stdev = math.sqrt(sum(r * r for r in residuals) / n)

    basis = fitted[-1]

    return {
        "basis": basis,
        "upper": basis + REG_DEV * stdev,
        "lower": basis - REG_DEV * stdev,
        "slope": slope,
        "stdev": stdev,
        "bar_ts": int(window[-1]["timestamp"]),
    }

def structure_stop(side, candles, bars):
    if len(candles) < bars:
        return None

    recent = candles[-bars:]

    if side == "LONG":
        return min(float(c["low"]) for c in recent)

    return max(float(c["high"]) for c in recent)

def is_tighter_sl(side, current_sl, candidate):
    if candidate is None or candidate <= 0:
        return False

    if not current_sl or current_sl <= 0:
        return True

    if side == "LONG":
        return candidate > current_sl

    return candidate < current_sl

# ============================================================
# POSITION CONTROL
# ============================================================

def amend_position_sl(position_id, new_sl, reason):
    if ctrader_client is None or not ctrader_state["account_authorized"]:
        return False

    digits = market_state["US100"]["digits"] or 2
    new_sl = round(float(new_sl), digits)

    req = ProtoOAAmendPositionSLTPReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.positionId = int(position_id)
    req.stopLoss = float(new_sl)

    print(
        f"[MANAGER] AMEND SL position={position_id} "
        f"sl={new_sl} reason={reason}",
        flush=True,
    )

    state = manager_state.get(str(position_id))
    if state:
        state["last_requested_sl"] = new_sl
        state["last_action"] = f"SL -> {new_sl} | {reason}"
        save_state()

    def errback(failure):
        set_error(f"AMEND SL {position_id}: {failure}")

    ctrader_client.send(req).addErrback(errback)

    send_telegram_message(
        f"đĄď¸ US100 V10 DEMO\n"
        f"SL â {new_sl:.2f}\n"
        f"{reason}"
    )
    return True

def close_position(position_id, volume_raw, reason):
    state = manager_state.get(str(position_id))
    if state and state.get("close_requested"):
        return False

    if ctrader_client is None or not ctrader_state["account_authorized"]:
        return False

    if state:
        state["close_requested"] = True
        state["last_action"] = f"CLOSE | {reason}"
        save_state()

    req = ProtoOAClosePositionReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.positionId = int(position_id)
    req.volume = int(volume_raw)

    print(
        f"[MANAGER] CLOSE position={position_id} "
        f"volume={volume_raw} reason={reason}",
        flush=True,
    )

    def errback(failure):
        st = manager_state.get(str(position_id))
        if st:
            st["close_requested"] = False
            save_state()
        set_error(f"CLOSE {position_id}: {failure}")

    ctrader_client.send(req).addErrback(errback)

    send_telegram_message(
        f"đŞ US100 V10 DEMO\n"
        f"Manager zamyka pozycjÄ.\n"
        f"{reason}"
    )
    return True

# ============================================================
# TRADE MANAGER
# ============================================================

def ensure_manager_for_position(position):
    pid = str(position["position_id"])

    if pid in manager_state:
        return manager_state[pid]

    # Tylko pozycje naszego bota.
    label = position.get("label") or ""
    last = trade_state.get("last_order") or {}

    last_pid = last.get("position_id")
    belongs_to_bot = (
        label == BOT_LABEL
        or (last_pid is not None and int(last_pid) == int(position["position_id"]))
    )

    if not belongs_to_bot:
        return None

    entry = float(position.get("price") or last.get("actual_entry") or last.get("entry") or 0.0)
    if entry <= 0:
        return None

    state = {
        "position_id": int(position["position_id"]),
        "side": position["side_name"],
        "entry": entry,
        "tv_tp_reference": last.get("strategy_tp"),
        "phase": "M15",
        "basis_reached": False,
        "last_m15_bar": None,
        "last_h1_bar": None,
        "last_requested_sl": float(position.get("stop_loss") or 0.0),
        "close_requested": False,
        "created_at": int(time.time()),
        "last_action": "MANAGER_ATTACHED",
    }

    manager_state[pid] = state
    save_state()

    send_telegram_message(
        f"đ¤ US100 V10 MANAGER\n"
        f"{state['side']} przejÄty.\n"
        f"Entry: {entry:.2f}\n"
        f"Faza: M15"
    )

    return state

def manage_position(position):
    state = ensure_manager_for_position(position)

    if not state or state.get("close_requested"):
        return

    side = state["side"]
    current_sl = float(
        position.get("stop_loss")
        or state.get("last_requested_sl")
        or 0.0
    )

    m15 = closed_candles("M15")
    h1 = closed_candles("H1")

    if len(m15) < REG_LENGTH:
        return

    m15_reg = regression_channel(m15)
    if not m15_reg:
        return

    last_m15 = m15[-1]
    m15_ts = int(last_m15["timestamp"])

    # Faza 1: M15 prowadzi pozycjÄ aĹź do osiÄgniÄcia basis.
    if not state.get("basis_reached"):
        reached = (
            float(last_m15["high"]) >= float(m15_reg["basis"])
            if side == "LONG"
            else float(last_m15["low"]) <= float(m15_reg["basis"])
        )

        if reached:
            state["basis_reached"] = True
            state["phase"] = "H1"
            state["basis_reached_at"] = m15_ts
            state["m15_basis_at_promotion"] = float(m15_reg["basis"])

            candidate = structure_stop(
                side,
                m15,
                M15_STRUCTURE_BARS,
            )

            if candidate is not None:
                close_price = float(last_m15["close"])
                valid = (
                    candidate < close_price
                    if side == "LONG"
                    else candidate > close_price
                )

                if valid and is_tighter_sl(side, current_sl, candidate):
                    amend_position_sl(
                        position["position_id"],
                        candidate,
                        "M15 basis osiÄgniÄty - SL po strukturze M15",
                    )

            send_telegram_message(
                f"đ US100 V10 MANAGER\n"
                f"M15 basis osiÄgniÄty.\n"
                f"{side}\n"
                f"Basis: {m15_reg['basis']:.2f}\n"
                f"âĄď¸ H1 przejmuje prowadzenie."
            )

            save_state()

    state["last_m15_bar"] = m15_ts

    # Faza 2: H1.
    if state.get("phase") != "H1":
        save_state()
        return

    if len(h1) < REG_LENGTH:
        save_state()
        return

    h1_reg = regression_channel(h1)
    if not h1_reg:
        save_state()
        return

    last_h1 = h1[-1]
    h1_ts = int(last_h1["timestamp"])

    # Tylko raz na kaĹźdej zamkniÄtej Ĺwiecy H1.
    if state.get("last_h1_bar") == h1_ts:
        save_state()
        return

    state["last_h1_bar"] = h1_ts

    h1_close = float(last_h1["close"])

    # JeĹli zamkniÄta H1 wraca po zĹÄ stronÄ basis -> zamkniÄcie.
    invalidated = (
        h1_close < float(h1_reg["basis"])
        if side == "LONG"
        else h1_close > float(h1_reg["basis"])
    )

    if invalidated:
        close_position(
            position["position_id"],
            position["volume_raw"],
            f"H1 zamkniÄta po zĹej stronie basis {h1_reg['basis']:.2f}",
        )
        save_state()
        return

    # JeĹźeli H1 nadal trzyma ruch, podciÄgamy SL po strukturze.
    candidate = structure_stop(
        side,
        h1,
        H1_STRUCTURE_BARS,
    )

    if candidate is not None:
        valid = (
            candidate < h1_close
            if side == "LONG"
            else candidate > h1_close
        )

        if valid and is_tighter_sl(side, current_sl, candidate):
            amend_position_sl(
                position["position_id"],
                candidate,
                "H1 trzyma kierunek - SL po strukturze H1",
            )
        else:
            print(
                f"[MANAGER] H1 HOLD position={position['position_id']} "
                f"{side} close={h1_close:.2f} basis={h1_reg['basis']:.2f}",
                flush=True,
            )

    save_state()

def run_trade_manager():
    if not ctrader_state["account_authorized"]:
        return

    if not ctrader_state["market_ready"]:
        return

    symbol_id = market_state["US100"]["symbol_id"]
    if not symbol_id:
        return

    open_ids = set()

    for position in list(ctrader_state["positions"]):
        if int(position.get("symbol_id") or 0) != int(symbol_id):
            continue

        open_ids.add(str(position["position_id"]))
        manage_position(position)

    changed = False

    for pid in list(manager_state.keys()):
        if pid not in open_ids:
            del manager_state[pid]
            changed = True

    if changed:
        save_state()

# ============================================================
# SEND ORDER
# ============================================================

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
    strategy_sl = signal["strategy_sl"]
    strategy_tp = signal["strategy_tp"]

    if signal["side"] == "LONG" and not (strategy_sl < entry < strategy_tp):
        print("[TV] BLOCKED: invalid LONG SL/TP", flush=True)
        return

    if signal["side"] == "SHORT" and not (strategy_tp < entry < strategy_sl):
        print("[TV] BLOCKED: invalid SHORT SL/TP", flush=True)
        return

    signal_key = (
        f"{signal['side']}:{signal['timeframe']}:"
        f"{entry:.5f}:{strategy_sl:.5f}:{strategy_tp:.5f}"
    )

    if trade_state["last_signal_key"].get("US100") == signal_key:
        print("[TV] BLOCKED: duplicate signal", flush=True)
        return

    volume_raw = fixed_volume_for_lots(FIXED_LOTS_US100)

    if volume_raw is None:
        print(
            f"[TV] BLOCKED: exact {FIXED_LOTS_US100:.2f} lot volume unavailable",
            flush=True,
        )
        send_telegram_message(
            f"â ď¸ US100 V10 DEMO\n"
            f"Nie mogÄ ustawiÄ dokĹadnie {FIXED_LOTS_US100:.2f} lot."
        )
        return

    # Awaryjny SL 0.70%. TP strategii NIE jest wysyĹany do cTrader.
    if signal["side"] == "LONG":
        emergency_sl = entry * (1.0 - EMERGENCY_SL_PCT)
    else:
        emergency_sl = entry * (1.0 + EMERGENCY_SL_PCT)

    sl_distance = abs(entry - emergency_sl)

    req = ProtoOANewOrderReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.symbolId = int(symbol_id)
    req.orderType = ProtoOAOrderType.MARKET
    req.tradeSide = (
        ProtoOATradeSide.BUY
        if signal["side"] == "LONG"
        else ProtoOATradeSide.SELL
    )
    req.volume = int(volume_raw)

    # PoczÄtkowy awaryjny SL. Brak relativeTakeProfit.
    # cTrader wymaga relativeStopLoss zgodnego z dozwoloną precyzją.
    # Zaokrąglamy dystans SL do 0.01 punktu US100.
    sl_distance = round(sl_distance, 2)

    req.relativeStopLoss = max(
        1,
        int(round(sl_distance * 100000.0))
    )

    req.label = BOT_LABEL
    req.comment = (
        f"TV US100 M15 {signal['side']} "
        f"{FIXED_LOTS_US100:.2f} lot V10 MANAGER"
    )[:512]
    req.clientOrderId = (
        f"V10-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    )[:50]

    trade_state["pending_symbols"].add("US100")
    trade_state["last_signal_key"]["US100"] = signal_key

    trade_state["last_order"] = {
        "side": signal["side"],
        "entry": entry,
        "strategy_sl": strategy_sl,
        "strategy_tp": strategy_tp,
        "emergency_sl": emergency_sl,
        "lots": FIXED_LOTS_US100,
        "volume_raw": volume_raw,
        "status": "SENT",
        "sent_at": int(time.time()),
    }

    save_state()

    print("[ORDER SEND]", trade_state["last_order"], flush=True)

    send_telegram_message(
        f"đĽ US100 {signal['side']} â M15\n"
        f"Cena: {entry:.2f}\n"
        f"Awaryjny SL 0.70%: {emergency_sl:.2f}\n"
        f"TP strategii: {strategy_tp:.2f} (referencja)\n"
        f"Wolumen: {FIXED_LOTS_US100:.2f} lot\n"
        f"đ¤ Manager M15 â H1\n"
        f"âĄď¸ cTrader DEMO"
    )

    def order_errback(failure):
        trade_state["pending_symbols"].discard("US100")
        trade_state["last_order_error"] = str(failure)
        set_error(failure)

    ctrader_client.send(req).addErrback(order_errback)

# ============================================================
# PROCESS ALERT
# ============================================================

def process_strategy_alert(text):
    print(
        "[TV] RAW ALERT:",
        str(text).replace("\n", " | ")[:1000],
        flush=True,
    )

    signal = parse_strategy_alert(text)

    print(
        f"[TV] PARSED event={signal['event']} "
        f"side={signal['side']} "
        f"symbol={signal['symbol']} "
        f"tf={signal['timeframe']} "
        f"entry={signal['strategy_entry']} "
        f"sl={signal['strategy_sl']} "
        f"tp={signal['strategy_tp']}",
        flush=True,
    )

    if signal["event"] == "GENERIC_FILL":
        print("[TV] REJECTED: generic fill lacks strategy Cena/SL/TP", flush=True)
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

    if None in (
        signal["strategy_entry"],
        signal["strategy_sl"],
        signal["strategy_tp"],
    ):
        print("[TV] REJECTED: missing Cena/SL/TP", flush=True)
        return

    if not getattr(reactor, "running", False):
        print("[TV] REJECTED: reactor not running", flush=True)
        return

    print("[TV] ACCEPTED -> CTRADER DEMO V10", flush=True)

    reactor.callFromThread(
        submit_strategy_market_order,
        signal,
    )

# ============================================================
# TREND BARS
# ============================================================

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

    req.period = (
        ProtoOATrendbarPeriod.M15
        if timeframe == "M15"
        else ProtoOATrendbarPeriod.H1
    )

    # 14 dni daje wystarczajÄcy zapas dla 100 Ĺwiec H1.
    req.fromTimestamp = now_ms - 14 * 86400000
    req.toTimestamp = now_ms
    req.count = 250

    client.send(req).addErrback(safe_errback)

# ============================================================
# ACCOUNT DATA
# ============================================================

def request_account_data(client):
    if not ctrader_state["account_authorized"]:
        return

    account_id = int(ctrader_state["account_id"])

    if not market_state["US100"]["found"]:
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = account_id
        req.includeArchivedSymbols = False
        client.send(req).addErrback(safe_errback)

    for req in (
        ProtoOATraderReq(),
        ProtoOAReconcileReq(),
        ProtoOAGetPositionUnrealizedPnLReq(),
    ):
        req.ctidTraderAccountId = account_id
        client.send(req).addErrback(safe_errback)

# ============================================================
# REFRESH
# ============================================================

def refresh_market():
    if ctrader_client is None or not ctrader_state["account_authorized"]:
        return

    request_account_data(ctrader_client)

    if market_state["US100"]["digits"] is not None:
        request_trendbars(ctrader_client, "M15")
        request_trendbars(ctrader_client, "H1")
        reactor.callLater(2.0, run_trade_manager)

def start_refresh_loop():
    global refresh_started

    if refresh_started:
        return

    refresh_started = True

    def tick():
        refresh_market()
        reactor.callLater(AUTO_REFRESH_SECONDS, tick)

    reactor.callLater(5.0, tick)

# ============================================================
# AUTH
# ============================================================

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

# ============================================================
# CTRADER CONNECTION
# ============================================================

def start_ctrader_connection():
    global ctrader_client

    if ctrader_client is not None and ctrader_state["connected"]:
        send_account_list(ctrader_client)
        return

    # HARD DEMO ENDPOINT
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

        # APPLICATION AUTH
        if payload_type == ProtoOAApplicationAuthRes().payloadType:
            ctrader_state["application_authorized"] = True
            send_account_list(client)

        # ACCOUNT LIST
        elif payload_type == ProtoOAGetAccountListByAccessTokenRes().payloadType:
            response = Protobuf.extract(message)

            matches = [
                account
                for account in response.ctidTraderAccount
                if int(getattr(account, "traderLogin", 0) or 0)
                == TARGET_TRADER_LOGIN
            ]

            if not matches:
                set_error(f"Nie znaleziono traderLogin={TARGET_TRADER_LOGIN}")
                return

            account = matches[0]

            # ABSOLUTE LIVE BLOCK
            if bool(getattr(account, "isLive", False)):
                ctrader_state["account_id"] = None
                ctrader_state["account_authorized"] = False
                set_error("LIVE BLOCKED - ten bot jest tylko DEMO")
                return

            ctrader_state["account_id"] = int(account.ctidTraderAccountId)

            print(
                "[CTRADER] DEMO TARGET FOUND "
                f"login={TARGET_TRADER_LOGIN} "
                f"account_id={ctrader_state['account_id']}",
                flush=True,
            )

            authorize_account(client)

        # ACCOUNT AUTH
        elif payload_type == ProtoOAAccountAuthRes().payloadType:
            ctrader_state["account_authorized"] = True
            clear_error()

            print("[CTRADER] ACCOUNT AUTHORIZED DEMO", flush=True)

            request_account_data(client)
            start_refresh_loop()

        # DISCONNECT
        elif payload_type == ProtoOAAccountDisconnectEvent().payloadType:
            ctrader_state["account_authorized"] = False
            reactor.callLater(1.0, authorize_account, client)

        # TOKEN INVALID
        elif payload_type == ProtoOAAccountsTokenInvalidatedEvent().payloadType:
            ctrader_state["account_authorized"] = False
            set_error(
                "Token cTrader wygasĹ - zaloguj ponownie przez /ctrader/login"
            )

        # TRADER
        elif payload_type == ProtoOATraderRes().payloadType:
            response = Protobuf.extract(message)
            trader = response.trader
            divisor = 10 ** trader.moneyDigits

            ctrader_state["balance"] = round(
                trader.balance / divisor,
                2,
            )

            if ctrader_state["equity"] is None:
                ctrader_state["equity"] = ctrader_state["balance"]

        # PNL
        elif payload_type == ProtoOAGetPositionUnrealizedPnLRes().payloadType:
            response = Protobuf.extract(message)
            divisor = 10 ** response.moneyDigits

            pnl = sum(
                x.netUnrealizedPnL
                for x in response.positionUnrealizedPnL
            ) / divisor

            ctrader_state["unrealized_pnl"] = round(pnl, 2)

            if ctrader_state["balance"] is not None:
                ctrader_state["equity"] = round(
                    ctrader_state["balance"] + pnl,
                    2,
                )

        # POSITIONS
        elif payload_type == ProtoOAReconcileRes().payloadType:
            response = Protobuf.extract(message)

            positions = []

            for position in response.position:
                td = position.tradeData
                side_value = int(td.tradeSide)

                side_name = (
                    "LONG"
                    if side_value == int(ProtoOATradeSide.BUY)
                    else "SHORT"
                )

                positions.append({
                    "position_id": int(position.positionId),
                    "symbol_id": int(td.symbolId),
                    "volume_raw": int(td.volume),
                    "side": side_value,
                    "side_name": side_name,
                    "label": str(getattr(td, "label", "") or ""),
                    "price": float(getattr(position, "price", 0.0) or 0.0),
                    "stop_loss": float(getattr(position, "stopLoss", 0.0) or 0.0),
                    "take_profit": float(getattr(position, "takeProfit", 0.0) or 0.0),
                })

            ctrader_state["positions"] = positions

            ctrader_state["orders"] = [
                int(order.orderId)
                for order in response.order
            ]

            reactor.callLater(0.2, run_trade_manager)

        # SYMBOL LIST
        elif payload_type == ProtoOASymbolsListRes().payloadType:
            response = Protobuf.extract(message)

            candidates = [
                symbol
                for symbol in response.symbol
                if detect_instrument(symbol.symbolName) == "US100"
            ]

            if not candidates:
                set_error("Nie znaleziono US100 / US TECH 100")
                return

            exact = [
                s
                for s in candidates
                if normalize_symbol(s.symbolName) == "US100"
            ]

            symbol = exact[0] if exact else candidates[0]

            data = market_state["US100"]
            data["found"] = True
            data["symbol_id"] = int(symbol.symbolId)
            data["symbol_name"] = symbol.symbolName

            req = ProtoOASymbolByIdReq()
            req.ctidTraderAccountId = int(ctrader_state["account_id"])
            req.symbolId.append(int(symbol.symbolId))

            client.send(req).addErrback(safe_errback)

        # SYMBOL DETAILS
        elif payload_type == ProtoOASymbolByIdRes().payloadType:
            response = Protobuf.extract(message)

            for symbol in response.symbol:
                if int(symbol.symbolId) != int(
                    market_state["US100"]["symbol_id"]
                ):
                    continue

                data = market_state["US100"]

                data["digits"] = int(symbol.digits)
                data["min_volume_raw"] = int(
                    getattr(symbol, "minVolume", 0) or 0
                )
                data["max_volume_raw"] = int(
                    getattr(symbol, "maxVolume", 0) or 0
                )
                data["step_volume_raw"] = int(
                    getattr(symbol, "stepVolume", 0) or 0
                )
                data["lot_size_raw"] = int(
                    getattr(symbol, "lotSize", 0) or 0
                )

                print(
                    "[CTRADER] SYMBOL READY "
                    f"{data['symbol_name']} "
                    f"lotSize={data['lot_size_raw']} "
                    f"min={data['min_volume_raw']} "
                    f"max={data['max_volume_raw']} "
                    f"step={data['step_volume_raw']}",
                    flush=True,
                )

                request_trendbars(client, "M15")
                request_trendbars(client, "H1")

        # TREND BARS
        elif payload_type == ProtoOAGetTrendbarsRes().payloadType:
            response = Protobuf.extract(message)

            if int(response.symbolId) != int(
                market_state["US100"]["symbol_id"]
            ):
                return

            timeframe = (
                "M15"
                if response.period == ProtoOATrendbarPeriod.M15
                else "H1"
                if response.period == ProtoOATrendbarPeriod.H1
                else None
            )

            if timeframe is None:
                return

            digits = market_state["US100"]["digits"] or 2

            candles = [
                trendbar_to_dict(bar, digits)
                for bar in response.trendbar
            ]

            candles.sort(key=lambda item: item["timestamp"])

            market_state["US100"]["candles"][timeframe] = candles

            ctrader_state["market_ready"] = (
                len(market_state["US100"]["candles"]["M15"]) >= REG_LENGTH
                and len(market_state["US100"]["candles"]["H1"]) >= REG_LENGTH
            )

            ctrader_state["last_market_refresh"] = int(time.time())

            if ctrader_state["market_ready"]:
                reactor.callLater(0.2, run_trade_manager)

        # EXECUTION
        elif payload_type == ProtoOAExecutionEvent().payloadType:
            response = Protobuf.extract(message)

            symbol_id = None

            if response.HasField("order"):
                symbol_id = int(
                    response.order.tradeData.symbolId
                )
            elif response.HasField("position"):
                symbol_id = int(
                    response.position.tradeData.symbolId
                )

            if symbol_id == market_state["US100"]["symbol_id"]:
                trade_state["pending_symbols"].discard("US100")

            if int(response.executionType) == ProtoOAExecutionType.ORDER_FILLED:
                last = trade_state.get("last_order") or {}

                if last.get("status") == "SENT":
                    last["status"] = "FILLED"

                    if response.HasField("position"):
                        last["position_id"] = int(
                            response.position.positionId
                        )

                        last["actual_entry"] = float(
                            getattr(
                                response.position,
                                "price",
                                0.0,
                            ) or 0.0
                        )

                    save_state()

                    print("[ORDER FILLED] US100 V10", flush=True)

                    send_telegram_message(
                        "â US100 V10 DEMO\n"
                        "Pozycja zostaĹa otwarta.\n"
                        "đ¤ Manager M15 â H1 aktywny."
                    )

                reactor.callLater(
                    0.5,
                    request_account_data,
                    client,
                )

            elif int(response.executionType) == ProtoOAExecutionType.ORDER_REJECTED:
                if trade_state.get("last_order"):
                    trade_state["last_order"]["status"] = "REJECTED"

                trade_state["last_order_error"] = str(
                    getattr(
                        response,
                        "errorCode",
                        "ORDER_REJECTED",
                    )
                )

            else:
                reactor.callLater(
                    0.5,
                    request_account_data,
                    client,
                )

        # ORDER ERROR
        elif payload_type == ProtoOAOrderErrorEvent().payloadType:
            response = Protobuf.extract(message)

            trade_state["pending_symbols"].clear()

            trade_state["last_order_error"] = {
                "code": str(response.errorCode),
                "description": str(response.description),
            }

            set_error(
                f"{response.errorCode}: {response.description}"
            )

        # GENERIC ERROR
        elif payload_type == ProtoOAErrorRes().payloadType:
            response = Protobuf.extract(message)

            code = str(response.errorCode)
            description = str(response.description)

            if (
                "ALREADY_LOGGED_IN" in code
                or "ALREADY_LOGGED_IN" in description
            ):
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

# ============================================================
# REACTOR
# ============================================================

def start_reactor():
    global reactor_started

    if reactor_started:
        return

    reactor_started = True

    reactor.run(
        installSignalHandlers=False
    )

# ============================================================
# BOOT
# ============================================================

def bootstrap():
    load_state()

    if not ctrader_state.get("access_token"):
        print(
            "[BOOT] Login required: /ctrader/login",
            flush=True,
        )
        return

    threading.Thread(
        target=start_reactor,
        daemon=True,
    ).start()

    for _ in range(50):
        if getattr(reactor, "running", False):
            break

        time.sleep(0.1)

    if getattr(reactor, "running", False):
        reactor.callFromThread(
            start_ctrader_connection
        )

# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():
    return jsonify({
        "bot": "US100 V10 DEMO",
        "status": "ONLINE",
        "demo_only": True,
        "target_trader_login": TARGET_TRADER_LOGIN,
        "fixed_lots_us100": FIXED_LOTS_US100,
        "emergency_sl_pct": EMERGENCY_SL_PCT * 100.0,
        "regression_length": REG_LENGTH,
        "regression_deviation": REG_DEV,
        "connected": ctrader_state["connected"],
        "account_authorized": ctrader_state["account_authorized"],
        "market_ready": ctrader_state["market_ready"],
        "balance": ctrader_state["balance"],
        "equity": ctrader_state["equity"],
        "last_order": trade_state["last_order"],
        "manager_state": manager_state,
        "error": ctrader_state["error"],
        "state_dir": BOT_STATE_DIR,
    })

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "demo_only": True,
        "version": "V10",
    })

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
        "m15_bars": len(data["candles"]["M15"]),
        "h1_bars": len(data["candles"]["H1"]),
        "last_error": ctrader_state["error"],
    })

@app.route("/trade/status")
def trade_status():
    return jsonify({
        "pending_symbols": list(
            trade_state["pending_symbols"]
        ),
        "positions": ctrader_state["positions"],
        "last_order": trade_state["last_order"],
        "last_order_error": trade_state["last_order_error"],
        "manager_state": manager_state,
    })

@app.route("/telegram/test")
def telegram_test():
    ok = send_telegram_message(
        "đ§Ş US100 V10 DEMO\n"
        "Telegram dziaĹa poprawnie.\n"
        "2.00 loty | Manager M15 â H1."
    )

    return jsonify({
        "status": "success" if ok else "error",
        "telegram_sent": ok,
    })

@app.route("/webhook", methods=["POST"])
def webhook():
    if (
        WEBHOOK_SECRET
        and request.args.get("secret") != WEBHOOK_SECRET
    ):
        return jsonify({
            "status": "error",
            "message": "invalid secret",
        }), 403

    if request.is_json:
        data = request.get_json(silent=True)

        if isinstance(data, dict):
            text = (
                data.get("message")
                or data.get("text")
                or json.dumps(data)
            )
        else:
            text = str(data or "")
    else:
        text = request.get_data(as_text=True)

    if not text or not text.strip():
        return jsonify({
            "status": "error",
            "message": "empty alert",
        }), 400

    print(
        "[WEBHOOK] RECEIVED "
        f"content_type={request.content_type} "
        f"bytes={len(text.encode('utf-8', errors='ignore'))}",
        flush=True,
    )

    threading.Thread(
        target=process_strategy_alert,
        args=(text,),
        daemon=True,
    ).start()

    return jsonify({
        "status": "accepted",
        "mode": "US100_V10_DEMO",
    }), 200

@app.route("/market/refresh")
def market_refresh():
    if (
        ctrader_client is None
        or not ctrader_state["account_authorized"]
    ):
        return jsonify({
            "status": "error",
            "message": "cTrader not authorized",
        }), 503

    reactor.callFromThread(
        refresh_market
    )

    return jsonify({
        "status": "refresh_started",
    })

@app.route("/manager/run")
def manager_run():
    if (
        ctrader_client is None
        or not ctrader_state["account_authorized"]
    ):
        return jsonify({
            "status": "error",
            "message": "cTrader not authorized",
        }), 503

    reactor.callFromThread(
        run_trade_manager
    )

    return jsonify({
        "status": "manager_started",
        "demo_only": True,
    })

@app.route("/ctrader/login")
def ctrader_login():
    return redirect(
        "https://id.ctrader.com/"
        "my/settings/openapi/"
        "grantingaccess/?"
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
        return jsonify({
            "status": "error",
            "message": "Brak kodu OAuth",
        }), 400

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

        ctrader_state["access_token"] = data.get(
            "accessToken"
        )

        ctrader_state["refresh_token"] = data.get(
            "refreshToken"
        )

        save_state()

    except Exception as error:
        set_error(error)

        return jsonify({
            "status": "error",
            "message": "Token error",
        }), 500

    if not reactor_started:
        threading.Thread(
            target=start_reactor,
            daemon=True,
        ).start()

        time.sleep(0.5)

    reactor.callFromThread(
        start_ctrader_connection
    )

    return jsonify({
        "status": "success",
        "route": "DEMO",
        "target_trader_login": TARGET_TRADER_LOGIN,
        "version": "V10",
    })

# ============================================================
# START
# ============================================================

threading.Thread(
    target=bootstrap,
    daemon=True,
).start()

if __name__ == "__main__":
    port = int(
        os.environ.get(
            "PORT",
            "10000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
