# -*- coding: utf-8 -*-

"""
============================================================
US100 V11 DEMO - SIGNAL OBSERVER

TradingView -> Webhook -> Telegram
                       -> cTrader DEMO market data
                       -> Regression analysis -> Telegram

KONTO DEMO: 5901967

ZASADY:
- NIE otwiera transakcji
- NIE zamyka transakcji
- NIE zmienia SL ani TP
- NIE zmienia strategii TradingView
- ENTRY / SL / TP zawsze pochodza z alertu
- Telegram dziala niezaleznie od danych cTrader
- Analiza rynku jest dodatkowa

WEBHOOK:
https://us100-ai-manager.onrender.com/webhook?secret=...

============================================================
"""

import os
import re
import time
import math
import json
import tempfile
import threading
import unicodedata

from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

from flask import Flask, jsonify, redirect, request

from ctrader_open_api import (
    Client,
    Protobuf,
    TcpProtocol,
    EndPoints,
)

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOAGetAccountListByAccessTokenRes,
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
    ProtoOAAccountDisconnectEvent,
    ProtoOAAccountsTokenInvalidatedEvent,
    ProtoOASymbolsListReq,
    ProtoOASymbolsListRes,
    ProtoOASymbolByIdReq,
    ProtoOASymbolByIdRes,
    ProtoOAGetTrendbarsReq,
    ProtoOAGetTrendbarsRes,
    ProtoOAErrorRes,
)

from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOATrendbarPeriod,
)

from twisted.internet import reactor


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

BOT_NAME = "US100 V11 DEMO"

TARGET_TRADER_LOGIN = 5901967

FIXED_LOTS_US100 = float(
    os.getenv("FIXED_LOTS_US100", "2.00")
)

EMERGENCY_SL_PCT = float(
    os.getenv("EMERGENCY_SL_PCT", "0.0070")
)

REG_LENGTH = max(
    5,
    int(os.getenv("REG_LENGTH", "100"))
)

REG_DEV = float(
    os.getenv("REG_DEV", "2.0")
)

AUTO_REFRESH_SECONDS = max(
    30,
    int(os.getenv("AUTO_REFRESH_SECONDS", "60"))
)

# Wersja obserwacyjna.
# Zadna wartosc zmiennej srodowiskowej nie wlaczy handlu.
ENABLE_DEMO_TRADING = False

CTRADER_CLIENT_ID = os.getenv(
    "CTRADER_CLIENT_ID", ""
)

CTRADER_CLIENT_SECRET = os.getenv(
    "CTRADER_CLIENT_SECRET", ""
)

CTRADER_REDIRECT_URI = os.getenv(
    "CTRADER_REDIRECT_URI", ""
)

TELEGRAM_TOKEN = os.getenv(
    "TELEGRAM_TOKEN", ""
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID", ""
)

WEBHOOK_SECRET = os.getenv(
    "WEBHOOK_SECRET", ""
)


# ============================================================
# STATE
# ============================================================

def choose_state_dir():

    configured = os.getenv("BOT_STATE_DIR")

    candidates = (
        [configured]
        if configured
        else ["/var/data", "/tmp/us100-v11"]
    )

    for path in candidates:

        if not path:
            continue

        try:

            os.makedirs(path, exist_ok=True)

            probe = os.path.join(
                path,
                ".write_test"
            )

            with open(probe, "w") as f:
                f.write("ok")

            os.remove(probe)

            return path

        except Exception:
            continue

    return tempfile.gettempdir()


BOT_STATE_DIR = choose_state_dir()

# Zachowujemy zgodnosc z poprzednim plikiem tokenow.
BOT_STATE_FILE = os.path.join(
    BOT_STATE_DIR,
    "us100_v10_state.json"
)

state_lock = threading.RLock()

market_lock = threading.RLock()

ctrader_state = {
    "access_token": None,
    "refresh_token": None,
    "connected": False,
    "application_authorized": False,
    "account_authorized": False,
    "account_id": None,
    "market_ready": False,
    "last_market_refresh": None,
    "error": None,
}

market_state = {
    "US100": {
        "found": False,
        "symbol_id": None,
        "symbol_name": None,
        "digits": None,
        "candles": {
            "M15": [],
            "H1": [],
        },
    }
}

observer_state = {
    "signals_received": 0,
    "signals_forwarded": 0,
    "telegram_errors": 0,
    "last_signal": None,
    "last_analysis": None,
    "last_signal_time": None,
}

ctrader_client = None

reactor_started = False

refresh_started = False


# ============================================================
# HELPERS
# ============================================================

def set_error(error):

    ctrader_state["error"] = str(error)

    print(
        "[ERROR]",
        str(error),
        flush=True
    )


def safe_errback(failure):

    set_error(str(failure))


def fmt_price(value):

    if value is None:
        return "BRAK DANYCH"

    try:

        return (
            f"{float(value):,.2f}"
            .replace(",", " ")
        )

    except (ValueError, TypeError):
        return "BRAK DANYCH"


def normalize_symbol(name):

    return re.sub(
        r"[^A-Z0-9]",
        "",
        str(name or "").upper()
    )


def detect_instrument(name):

    value = normalize_symbol(name)

    aliases = (
        "US100",
        "NAS100",
        "USTEC",
        "USTECH100",
        "NASDAQ100",
    )

    if any(alias in value for alias in aliases):
        return "US100"

    return None


def strip_accents(value):

    normalized = unicodedata.normalize(
        "NFKD",
        str(value)
    )

    return "".join(
        char
        for char in normalized
        if not unicodedata.combining(char)
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_message(message):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:

        print(
            "[TELEGRAM] Brak konfiguracji",
            flush=True
        )

        observer_state["telegram_errors"] += 1

        return False

    try:

        response = requests.post(
            (
                "https://api.telegram.org/bot"
                f"{TELEGRAM_TOKEN}/sendMessage"
            ),
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
            },
            timeout=15
        )

        response.raise_for_status()

        data = response.json()

        if not data.get("ok"):

            observer_state["telegram_errors"] += 1

            return False

        return True

    except Exception as error:

        observer_state["telegram_errors"] += 1

        print(
            "[TELEGRAM ERROR]",
            error,
            flush=True
        )

        return False


# ============================================================
# SAVE / LOAD
# ============================================================

def save_state():

    payload = {
        "access_token": ctrader_state["access_token"],
        "refresh_token": ctrader_state["refresh_token"],
        "observer_state": observer_state,
    }

    try:

        with state_lock:

            temporary = BOT_STATE_FILE + ".tmp"

            with open(
                temporary,
                "w",
                encoding="utf-8"
            ) as handle:

                json.dump(
                    payload,
                    handle,
                    ensure_ascii=False,
                    indent=2
                )

            os.replace(
                temporary,
                BOT_STATE_FILE
            )

        return True

    except Exception as error:

        print(
            "[STATE ERROR]",
            error,
            flush=True
        )

        return False


def load_state():

    try:

        with state_lock:

            with open(
                BOT_STATE_FILE,
                encoding="utf-8"
            ) as handle:

                payload = json.load(handle)

        ctrader_state["access_token"] = payload.get(
            "access_token"
        )

        ctrader_state["refresh_token"] = payload.get(
            "refresh_token"
        )

        saved_observer = payload.get(
            "observer_state"
        )

        if isinstance(saved_observer, dict):

            observer_state.update(
                saved_observer
            )

        return True

    except FileNotFoundError:

        return False

    except Exception as error:

        print(
            "[STATE LOAD ERROR]",
            error,
            flush=True
        )

        return False


# ============================================================
# TRADINGVIEW PARSER
# ============================================================

def extract_number(label, text):

    match = re.search(
        rf"\b{re.escape(label)}\s*:\s*"
        r"(-?[0-9]+(?:[.,][0-9]+)?)",
        text,
        re.I
    )

    if not match:
        return None

    try:

        return float(
            match.group(1).replace(",", ".")
        )

    except ValueError:
        return None


def parse_strategy_alert(text):

    raw = str(text or "").strip()

    normalized = re.sub(
        r"\s+",
        " ",
        strip_accents(raw).upper()
    ).strip()

    if "WEJSCIE LONG" in normalized:

        side = "LONG"

    elif "WEJSCIE SHORT" in normalized:

        side = "SHORT"

    else:

        side = None

    symbol_match = re.search(
        r"\bSYMBOL\s*:\s*([^|]+)",
        normalized
    )

    timeframe_match = re.search(
        r"\bTF\s*:\s*([^|]+)",
        normalized
    )

    symbol_raw = (
        symbol_match.group(1).strip()
        if symbol_match
        else ""
    )

    timeframe = (
        timeframe_match.group(1).strip().lower()
        if timeframe_match
        else "?"
    )

    return {
        "event": "ENTRY" if side else "UNKNOWN",
        "side": side,
        "symbol": detect_instrument(symbol_raw),
        "timeframe": timeframe,
        "strategy_entry": extract_number(
            "CENA",
            normalized
        ),
        "strategy_sl": extract_number(
            "SL",
            normalized
        ),
        "strategy_tp": extract_number(
            "TP",
            normalized
        ),
        "raw": raw,
    }


# ============================================================
# SIGNAL VALIDATION
# ============================================================

def validate_signal(signal):

    if signal["event"] != "ENTRY":

        return False, "Nieznany typ alertu"

    if signal["side"] not in ("LONG", "SHORT"):

        return False, "Brak kierunku"

    if signal["symbol"] != "US100":

        return False, "Inny instrument"

    if signal["timeframe"] not in (
        "15",
        "15m",
        "15min",
        "m15"
    ):

        return False, "Inny interwal"

    for key in (
        "strategy_entry",
        "strategy_sl",
        "strategy_tp"
    ):

        value = signal[key]

        if value is None:

            return False, f"Brak {key}"

        if not math.isfinite(value):

            return False, f"Niepoprawne {key}"

        if value <= 0:

            return False, f"Niepoprawna cena {key}"

    return True, "OK"


# ============================================================
# TELEGRAM SIGNAL
# ============================================================

def telegram_strategy_signal(signal):

    side = signal["side"]

    entry = signal["strategy_entry"]

    strategy_sl = signal["strategy_sl"]

    strategy_tp = signal["strategy_tp"]

    if side == "LONG":

        emergency_sl = (
            entry * (1 - EMERGENCY_SL_PCT)
        )

        direction_icon = "🟢"

    else:

        emergency_sl = (
            entry * (1 + EMERGENCY_SL_PCT)
        )

        direction_icon = "🔴"

    message = (
        "📥 US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📡 SYGNAŁ TRADINGVIEW\n\n"

        f"{direction_icon} KIERUNEK: {side}\n"
        "📊 INTERWAŁ: M15\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📍 POZIOMY STRATEGII\n\n"

        f"💰 ENTRY: {fmt_price(entry)}\n"
        f"🛑 SL: {fmt_price(strategy_sl)}\n"
        f"🎯 TP: {fmt_price(strategy_tp)}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "⚙️ PARAMETRY DEMO\n\n"

        f"📦 Wolumen planowany: "
        f"{FIXED_LOTS_US100:.2f} lot\n"

        f"🚨 Awaryjny SL referencyjny: "
        f"{fmt_price(emergency_sl)}\n\n"

        "🤖 MANAGER: M15 → H1\n"
        "🔒 HANDEL AUTOMATYCZNY: OFF\n\n"

        "ℹ️ To sygnał strategii, "
        "nie potwierdzenie otwarcia pozycji.\n"

        "🧠 Analiza rynku w osobnej wiadomości."
    )

    return send_telegram_message(message)


# ============================================================
# REGRESSION
# ============================================================

def trendbar_to_dict(bar, digits):

    low = int(bar.low)

    return {
        "timestamp": (
            int(bar.utcTimestampInMinutes) * 60
        ),
        "open": round(
            (low + int(bar.deltaOpen)) / 100000.0,
            digits
        ),
        "high": round(
            (low + int(bar.deltaHigh)) / 100000.0,
            digits
        ),
        "low": round(
            low / 100000.0,
            digits
        ),
        "close": round(
            (low + int(bar.deltaClose)) / 100000.0,
            digits
        ),
        "volume": int(bar.volume),
    }


def closed_candles(timeframe):

    with market_lock:

        candles = list(
            market_state["US100"]["candles"].get(
                timeframe,
                []
            )
        )

    now = int(time.time())

    seconds = (
        900
        if timeframe == "M15"
        else 3600
    )

    return [
        candle
        for candle in candles
        if candle["timestamp"] + seconds <= now
    ]


def regression_channel(candles):

    if len(candles) < REG_LENGTH:

        return None

    window = candles[-REG_LENGTH:]

    prices = [
        float(candle["close"])
        for candle in window
    ]

    count = len(prices)

    mean_x = (count - 1) / 2.0

    mean_y = sum(prices) / count

    variance_x = sum(
        (index - mean_x) ** 2
        for index in range(count)
    )

    if variance_x <= 0:

        return None

    covariance = sum(
        (index - mean_x) * (prices[index] - mean_y)
        for index in range(count)
    )

    slope = covariance / variance_x

    intercept = mean_y - slope * mean_x

    fitted = [
        intercept + slope * index
        for index in range(count)
    ]

    residuals = [
        prices[index] - fitted[index]
        for index in range(count)
    ]

    stdev = math.sqrt(
        sum(value * value for value in residuals)
        / count
    )

    basis = fitted[-1]

    return {
        "basis": basis,
        "upper": basis + REG_DEV * stdev,
        "lower": basis - REG_DEV * stdev,
        "slope": slope,
        "stdev": stdev,
        "bar_ts": window[-1]["timestamp"],
    }


# ============================================================
# MARKET ANALYSIS
# ============================================================

def analyse_market(signal):

    m15 = closed_candles("M15")

    h1 = closed_candles("H1")

    m15_reg = regression_channel(m15)

    h1_reg = regression_channel(h1)

    if not m15_reg or not h1_reg:

        return {
            "status": "WAITING",
            "reason": (
                "Dane M15/H1 nie sa jeszcze gotowe."
            )
        }

    price = float(m15[-1]["close"])

    m15_basis = m15_reg["basis"]

    h1_basis = h1_reg["basis"]

    h1_slope = h1_reg["slope"]

    side = signal["side"]

    if h1_slope > 0:

        trend = "WZROSTOWY"

    elif h1_slope < 0:

        trend = "SPADKOWY"

    else:

        trend = "BOCZNY"

    if side == "LONG":

        alignment = (
            h1_slope > 0
            and price >= h1_basis
        )

    else:

        alignment = (
            h1_slope < 0
            and price <= h1_basis
        )

    if alignment:

        decision = "ZGODNY Z H1"

        reason = (
            "Kierunek sygnalu jest zgodny "
            "z nachyleniem regresji H1, "
            "a cena znajduje sie po "
            "odpowiedniej stronie basis."
        )

    else:

        decision = "WYMAGA OBSERWACJI"

        reason = (
            "Sygnal nie ma pelnego "
            "potwierdzenia z tunelu H1. "
            "Mozliwy ruch kontrtrendowy."
        )

    return {
        "status": "READY",
        "decision": decision,
        "reason": reason,
        "trend": trend,
        "price": price,
        "m15_basis": m15_basis,
        "h1_basis": h1_basis,
        "m15_upper": m15_reg["upper"],
        "m15_lower": m15_reg["lower"],
        "h1_upper": h1_reg["upper"],
        "h1_lower": h1_reg["lower"],
        "h1_slope": h1_slope,
    }


def telegram_market_analysis(signal, result):

    if result["status"] != "READY":

        return send_telegram_message(
            "🧠 US100 V11 DEMO\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📊 ANALIZA RYNKU\n\n"
            "⏳ Dane M15/H1 nie są jeszcze gotowe.\n\n"
            "✅ Sygnał ENTRY / SL / TP "
            "został obsłużony niezależnie.\n"
            "🔒 Brak automatycznych transakcji."
        )

    message = (
        "🧠 US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📊 ANALIZA TUNELU REGRESJI\n\n"

        f"📡 Sygnał: {signal['side']}\n"

        f"💰 ENTRY strategii: "
        f"{fmt_price(signal['strategy_entry'])}\n"

        f"🛑 SL strategii: "
        f"{fmt_price(signal['strategy_sl'])}\n"

        f"🎯 TP strategii: "
        f"{fmt_price(signal['strategy_tp'])}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 M15\n\n"

        f"💵 Ostatnie zamknięcie: "
        f"{fmt_price(result['price'])}\n"

        f"🔹 Basis: "
        f"{fmt_price(result['m15_basis'])}\n"

        f"🔺 Górna banda: "
        f"{fmt_price(result['m15_upper'])}\n"

        f"🔻 Dolna banda: "
        f"{fmt_price(result['m15_lower'])}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 H1\n\n"

        f"🔹 Basis: "
        f"{fmt_price(result['h1_basis'])}\n"

        f"📈 Trend: {result['trend']}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🤖 OCENA ALGORYTMICZNA\n\n"

        f"📌 {result['decision']}\n\n"

        f"💬 {result['reason']}\n\n"

        "ℹ️ Analiza informacyjna.\n"
        "🔒 Nie otwarto transakcji."
    )

    return send_telegram_message(message)


# ============================================================
# PROCESS ALERT
# ============================================================

def process_strategy_alert(text):

    print(
        "[TV RAW]",
        str(text)[:1000],
        flush=True
    )

    signal = parse_strategy_alert(text)

    print(
        "[TV PARSED]",
        signal,
        flush=True
    )

    valid, reason = validate_signal(signal)

    if not valid:

        print(
            "[TV REJECTED]",
            reason,
            flush=True
        )

        return

    # =========================================
    # 1. ZAPIS SYGNALU
    # =========================================

    with state_lock:

        observer_state["signals_received"] += 1

        observer_state["last_signal"] = signal

        observer_state["last_signal_time"] = (
            datetime.now(timezone.utc).isoformat()
        )

    save_state()

    # =========================================
    # 2. TELEGRAM - ZAWSZE PIERWSZY
    # =========================================

    delivered = telegram_strategy_signal(
        signal
    )

    print(
        "[TV TELEGRAM SIGNAL]",
        delivered,
        flush=True
    )

    if delivered:

        with state_lock:

            observer_state["signals_forwarded"] += 1

        save_state()

    # =========================================
    # 3. ANALIZA - NIEZALEZNA
    # =========================================

    try:

        analysis = analyse_market(signal)

        with state_lock:

            observer_state["last_analysis"] = analysis

        save_state()

        telegram_market_analysis(
            signal,
            analysis
        )

    except Exception as error:

        print(
            "[ANALYSIS ERROR]",
            error,
            flush=True
        )

        send_telegram_message(
            "⚠️ US100 V11 DEMO\n"
            "Analiza rynku nie powiodła się.\n"
            "Sygnał TradingView został "
            "przetworzony osobno."
        )


# ============================================================
# CTRADER MARKET DATA
# ============================================================

def request_trendbars(client, timeframe):

    symbol_id = market_state["US100"]["symbol_id"]

    if not ctrader_state["account_authorized"]:
        return

    if not symbol_id:
        return

    req = ProtoOAGetTrendbarsReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.symbolId = int(symbol_id)

    req.period = (
        ProtoOATrendbarPeriod.M15
        if timeframe == "M15"
        else ProtoOATrendbarPeriod.H1
    )

    now_ms = int(time.time() * 1000)

    days = (
        35
        if timeframe == "M15"
        else 30
    )

    req.fromTimestamp = (
        now_ms - days * 86400000
    )

    req.toTimestamp = now_ms

    req.count = 1500

    client.send(req).addErrback(
        safe_errback
    )


def request_account_data(client):

    if not ctrader_state["account_authorized"]:
        return

    if market_state["US100"]["found"]:
        return

    req = ProtoOASymbolsListReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.includeArchivedSymbols = False

    client.send(req).addErrback(
        safe_errback
    )


def refresh_market():

    if ctrader_client is None:
        return

    if not ctrader_state["account_authorized"]:
        return

    request_account_data(ctrader_client)

    if market_state["US100"]["digits"] is None:
        return

    request_trendbars(
        ctrader_client,
        "M15"
    )

    request_trendbars(
        ctrader_client,
        "H1"
    )


def start_refresh_loop():

    global refresh_started

    if refresh_started:
        return

    refresh_started = True

    def tick():

        refresh_market()

        reactor.callLater(
            AUTO_REFRESH_SECONDS,
            tick
        )

    reactor.callLater(
        5.0,
        tick
    )


# ============================================================
# CTRADER AUTH
# ============================================================

def send_account_list(client):

    token = ctrader_state["access_token"]

    if not token:

        set_error("Brak access_token")

        return

    req = ProtoOAGetAccountListByAccessTokenReq()

    req.accessToken = token

    client.send(req).addErrback(
        safe_errback
    )


def authorize_account(client):

    account_id = ctrader_state["account_id"]

    if not account_id:
        return

    req = ProtoOAAccountAuthReq()

    req.ctidTraderAccountId = int(
        account_id
    )

    req.accessToken = ctrader_state[
        "access_token"
    ]

    client.send(req).addErrback(
        safe_errback
    )


# ============================================================
# CTRADER CONNECTION
# ============================================================

def start_ctrader_connection():

    global ctrader_client

    if (
        ctrader_client is not None
        and ctrader_state["connected"]
    ):

        send_account_list(
            ctrader_client
        )

        return

    ctrader_client = Client(
        EndPoints.PROTOBUF_DEMO_HOST,
        EndPoints.PROTOBUF_PORT,
        TcpProtocol
    )

    def connected(client):

        ctrader_state["connected"] = True

        ctrader_state["error"] = None

        req = ProtoOAApplicationAuthReq()

        req.clientId = CTRADER_CLIENT_ID

        req.clientSecret = CTRADER_CLIENT_SECRET

        client.send(req).addErrback(
            safe_errback
        )

    def disconnected(client, reason):

        ctrader_state["connected"] = False

        ctrader_state["account_authorized"] = False

        ctrader_state["market_ready"] = False

        print(
            "[CTRADER DISCONNECTED]",
            reason,
            flush=True
        )

    def on_message(client, message):

        payload_type = message.payloadType

        # APPLICATION AUTH

        if payload_type == (
            ProtoOAApplicationAuthRes().payloadType
        ):

            ctrader_state[
                "application_authorized"
            ] = True

            send_account_list(client)

        # ACCOUNT LIST

        elif payload_type == (
            ProtoOAGetAccountListByAccessTokenRes().payloadType
        ):

            response = Protobuf.extract(message)

            matches = [
                account
                for account in response.ctidTraderAccount
                if int(
                    getattr(
                        account,
                        "traderLogin",
                        0
                    ) or 0
                ) == TARGET_TRADER_LOGIN
            ]

            if not matches:

                set_error(
                    "Nie znaleziono konta DEMO "
                    f"{TARGET_TRADER_LOGIN}"
                )

                return

            account = matches[0]

            if bool(
                getattr(
                    account,
                    "isLive",
                    False
                )
            ):

                ctrader_state["account_id"] = None

                ctrader_state[
                    "account_authorized"
                ] = False

                set_error(
                    "LIVE BLOCKED - DEMO ONLY"
                )

                return

            ctrader_state["account_id"] = int(
                account.ctidTraderAccountId
            )

            authorize_account(client)

        # ACCOUNT AUTH

        elif payload_type == (
            ProtoOAAccountAuthRes().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = True

            ctrader_state["error"] = None

            request_account_data(client)

            start_refresh_loop()

        # ACCOUNT DISCONNECT

        elif payload_type == (
            ProtoOAAccountDisconnectEvent().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = False

            reactor.callLater(
                1.0,
                authorize_account,
                client
            )

        # TOKEN INVALID

        elif payload_type == (
            ProtoOAAccountsTokenInvalidatedEvent().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = False

            set_error(
                "Token wygasl - /ctrader/login"
            )

        # SYMBOLS

        elif payload_type == (
            ProtoOASymbolsListRes().payloadType
        ):

            response = Protobuf.extract(message)

            candidates = [
                symbol
                for symbol in response.symbol
                if detect_instrument(
                    symbol.symbolName
                ) == "US100"
            ]

            if not candidates:

                set_error(
                    "Nie znaleziono US100"
                )

                return

            exact = [
                symbol
                for symbol in candidates
                if normalize_symbol(
                    symbol.symbolName
                ) == "US100"
            ]

            symbol = (
                exact[0]
                if exact
                else candidates[0]
            )

            data = market_state["US100"]

            data["found"] = True

            data["symbol_id"] = int(
                symbol.symbolId
            )

            data["symbol_name"] = (
                symbol.symbolName
            )

            req = ProtoOASymbolByIdReq()

            req.ctidTraderAccountId = int(
                ctrader_state["account_id"]
            )

            req.symbolId.append(
                int(symbol.symbolId)
            )

            client.send(req).addErrback(
                safe_errback
            )

        # SYMBOL DETAILS

        elif payload_type == (
            ProtoOASymbolByIdRes().payloadType
        ):

            response = Protobuf.extract(message)

            for symbol in response.symbol:

                if int(symbol.symbolId) != int(
                    market_state["US100"]["symbol_id"]
                ):

                    continue

                market_state["US100"]["digits"] = int(
                    symbol.digits
                )

                request_trendbars(
                    client,
                    "M15"
                )

                request_trendbars(
                    client,
                    "H1"
                )

        # TREND BARS

        elif payload_type == (
            ProtoOAGetTrendbarsRes().payloadType
        ):

            response = Protobuf.extract(message)

            symbol_id = market_state[
                "US100"
            ]["symbol_id"]

            if symbol_id is None:
                return

            if int(response.symbolId) != int(
                symbol_id
            ):

                return

            if response.period == (
                ProtoOATrendbarPeriod.M15
            ):

                timeframe = "M15"

            elif response.period == (
                ProtoOATrendbarPeriod.H1
            ):

                timeframe = "H1"

            else:

                return

            digits = market_state[
                "US100"
            ]["digits"]

            if digits is None:
                return

            candles = [
                trendbar_to_dict(
                    bar,
                    digits
                )
                for bar in response.trendbar
            ]

            candles.sort(
                key=lambda candle: candle["timestamp"]
            )

            with market_lock:

                market_state["US100"][
                    "candles"
                ][timeframe] = candles

            m15_ready = (
                len(closed_candles("M15"))
                >= REG_LENGTH
            )

            h1_ready = (
                len(closed_candles("H1"))
                >= REG_LENGTH
            )

            ctrader_state["market_ready"] = (
                m15_ready and h1_ready
            )

            ctrader_state[
                "last_market_refresh"
            ] = int(time.time())

        # GENERIC ERROR

        elif payload_type == (
            ProtoOAErrorRes().payloadType
        ):

            response = Protobuf.extract(message)

            code = str(
                response.errorCode
            )

            description = str(
                response.description
            )

            if "ALREADY_LOGGED_IN" in (
                code + description
            ):

                if ctrader_state["account_id"]:

                    ctrader_state[
                        "account_authorized"
                    ] = True

                    request_account_data(client)

                else:

                    send_account_list(client)

                return

            set_error(
                f"{code}: {description}"
            )

    ctrader_client.setConnectedCallback(
        connected
    )

    ctrader_client.setDisconnectedCallback(
        disconnected
    )

    ctrader_client.setMessageReceivedCallback(
        on_message
    )

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


def ensure_reactor():

    if not reactor_started:

        threading.Thread(
            target=start_reactor,
            daemon=True
        ).start()

    for _ in range(50):

        if getattr(reactor, "running", False):
            return True

        time.sleep(0.1)

    return False


# ============================================================
# BOOT
# ============================================================

def bootstrap():

    load_state()

    if not ctrader_state["access_token"]:

        print(
            "[BOOT] Login required: /ctrader/login",
            flush=True
        )

        return

    if ensure_reactor():

        reactor.callFromThread(
            start_ctrader_connection
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "bot": BOT_NAME,
        "status": "ONLINE",
        "version": "V11",
        "mode": "SIGNAL_OBSERVER",
        "demo_only": True,
        "trading_enabled": False,
        "target_trader_login": TARGET_TRADER_LOGIN,
        "fixed_lots_us100": FIXED_LOTS_US100,
        "emergency_sl_pct": EMERGENCY_SL_PCT * 100,
        "connected": ctrader_state["connected"],
        "account_authorized": ctrader_state[
            "account_authorized"
        ],
        "market_ready": ctrader_state[
            "market_ready"
        ],
        "observer": observer_state,
        "error": ctrader_state["error"],
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "version": "V11",
        "demo_only": True,
        "trading_enabled": False,
    })


@app.route("/ctrader/status")
def ctrader_status():

    return jsonify({
        "connected": ctrader_state["connected"],
        "account_authorized": ctrader_state[
            "account_authorized"
        ],
        "account_id": ctrader_state["account_id"],
        "target_trader_login": TARGET_TRADER_LOGIN,
        "route": "DEMO",
        "market_ready": ctrader_state[
            "market_ready"
        ],
        "last_market_refresh": ctrader_state[
            "last_market_refresh"
        ],
        "last_error": ctrader_state["error"],
    })


@app.route("/trade/status")
def trade_status():

    return jsonify({
        "trading_enabled": False,
        "mode": "OBSERVER_ONLY",
        "last_signal": observer_state[
            "last_signal"
        ],
        "last_analysis": observer_state[
            "last_analysis"
        ],
        "message": (
            "Ta wersja nie wysyla zlecen "
            "do cTrader."
        ),
    })


@app.route("/ai/status")
def ai_status():

    return jsonify({
        "mode": "ALGORITHMIC_ANALYSIS",
        "external_ai_enabled": False,
        "observer": observer_state,
    })


@app.route("/market")
def market():

    m15 = closed_candles("M15")

    h1 = closed_candles("H1")

    return jsonify({
        "market_ready": ctrader_state[
            "market_ready"
        ],
        "symbol": market_state[
            "US100"
        ]["symbol_name"],
        "m15_bars": len(m15),
        "h1_bars": len(h1),
        "M15_regression": regression_channel(m15),
        "H1_regression": regression_channel(h1),
    })


@app.route("/telegram/test")
def telegram_test():

    ok = send_telegram_message(
        "🧪 US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "✅ Telegram działa!\n\n"
        "📊 Instrument: US100\n"
        "📦 Wolumen planowany: 2.00 lot\n"
        "🛑 Awaryjny SL: 0.70%\n"
        "🎯 ENTRY / SL / TP: TradingView\n"
        "🤖 Analiza: M15 + H1\n\n"
        "🔒 HANDEL WYŁĄCZONY"
    )

    return jsonify({
        "status": (
            "success"
            if ok
            else "error"
        ),
        "telegram_sent": ok,
    })


# ============================================================
# WEBHOOK - ADRES BEZ ZMIAN
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    if not WEBHOOK_SECRET:

        return jsonify({
            "status": "error",
            "message": "WEBHOOK_SECRET not configured",
        }), 503

    if request.args.get("secret") != WEBHOOK_SECRET:

        return jsonify({
            "status": "error",
            "message": "invalid secret",
        }), 403

    if request.is_json:

        data = request.get_json(
            silent=True
        )

        if isinstance(data, dict):

            text = (
                data.get("message")
                or data.get("text")
                or json.dumps(
                    data,
                    ensure_ascii=False
                )
            )

        else:

            text = str(data or "")

    else:

        text = request.get_data(
            as_text=True
        )

    if not str(text).strip():

        return jsonify({
            "status": "error",
            "message": "empty alert",
        }), 400

    # Nie czekamy na cTrader.
    # Nie czekamy na regresje.
    # Nie sprawdzamy ENABLE_DEMO_TRADING.

    threading.Thread(
        target=process_strategy_alert,
        args=(text,),
        daemon=True
    ).start()

    return jsonify({
        "status": "accepted",
        "version": "V11",
        "mode": "SIGNAL_OBSERVER",
        "trading": False,
    }), 200


# ============================================================
# MARKET REFRESH
# ============================================================

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


# ============================================================
# MANAGER STATUS
# ============================================================

@app.route("/manager/run")
def manager_run():

    return jsonify({
        "status": "disabled",
        "version": "V11",
        "mode": "OBSERVER_ONLY",
        "message": (
            "Manager transakcji jest "
            "wylaczony w tej wersji."
        ),
    })


# ============================================================
# CTRADER LOGIN
# ============================================================

@app.route("/ctrader/login")
def ctrader_login():

    if not all((
        CTRADER_CLIENT_ID,
        CTRADER_CLIENT_SECRET,
        CTRADER_REDIRECT_URI
    )):

        return jsonify({
            "status": "error",
            "message": "Missing cTrader configuration",
        }), 503

    return redirect(
        "https://id.ctrader.com/"
        "my/settings/openapi/"
        "grantingaccess/?"
        + urlencode({
            "client_id": CTRADER_CLIENT_ID,
            "redirect_uri": CTRADER_REDIRECT_URI,
            "scope": "accounts",
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
            timeout=15
        )

        response.raise_for_status()

        data = response.json()

        ctrader_state["access_token"] = (
            data.get("accessToken")
        )

        ctrader_state["refresh_token"] = (
            data.get("refreshToken")
        )

        if not ctrader_state["access_token"]:

            raise RuntimeError(
                "Brak accessToken"
            )

        save_state()

    except Exception as error:

        set_error(error)

        return jsonify({
            "status": "error",
            "message": "Token error",
        }), 500

    if not ensure_reactor():

        return jsonify({
            "status": "error",
            "message": "Reactor not running",
        }), 503

    reactor.callFromThread(
        start_ctrader_connection
    )

    return jsonify({
        "status": "success",
        "route": "DEMO",
        "version": "V11",
        "trading_enabled": False,
        "target_trader_login": TARGET_TRADER_LOGIN,
    })


# ============================================================
# START
# ============================================================

threading.Thread(
    target=bootstrap,
    daemon=True
).start()


if __name__ == "__main__":

    port = int(
        os.getenv("PORT", "10000")
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
