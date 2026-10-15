# -*- coding: utf-8 -*-

# ============================================================
# US100 V11 DEMO
#
# TradingView -> cTrader DEMO -> Telegram
#
# Konto DEMO: 5901967
# Wolumen: 2.00 lot
# Awaryjny SL: 0.70%
# Brak sztywnego TP
#
# Trade Manager:
# M15 -> basis -> H1
#
# Telegram:
# - sygnal TradingView
# - potwierdzenie otwarcia
# - SL i TP strategii
# - zmiana SL
# - przejscie M15 -> H1
# - analiza regresji
# - potwierdzenie zamkniecia
#
# AI nie steruje transakcjami.
# Analiza jest algorytmiczna.
# ============================================================

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
    ProtoOATraderReq,
    ProtoOATraderRes,
    ProtoOAReconcileReq,
    ProtoOAReconcileRes,
    ProtoOASymbolsListReq,
    ProtoOASymbolsListRes,
    ProtoOASymbolByIdReq,
    ProtoOASymbolByIdRes,
    ProtoOAGetTrendbarsReq,
    ProtoOAGetTrendbarsRes,
    ProtoOAGetPositionUnrealizedPnLReq,
    ProtoOAGetPositionUnrealizedPnLRes,
    ProtoOANewOrderReq,
    ProtoOAExecutionEvent,
    ProtoOAOrderErrorEvent,
    ProtoOAErrorRes,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAClosePositionReq,
)

from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOATrendbarPeriod,
    ProtoOAOrderType,
    ProtoOATradeSide,
    ProtoOAExecutionType,
)

from twisted.internet import reactor


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

TARGET_TRADER_LOGIN = 5901967

FIXED_LOTS_US100 = float(
    os.environ.get("FIXED_LOTS_US100", "2.00")
)

BOT_LABEL = "US100_V10_DEMO"

EMERGENCY_SL_PCT = float(
    os.environ.get("EMERGENCY_SL_PCT", "0.0070")
)

REG_LENGTH = int(
    os.environ.get("REG_LENGTH", "100")
)

REG_DEV = float(
    os.environ.get("REG_DEV", "2.0")
)

M15_STRUCTURE_BARS = int(
    os.environ.get("M15_STRUCTURE_BARS", "4")
)

H1_STRUCTURE_BARS = int(
    os.environ.get("H1_STRUCTURE_BARS", "3")
)

AUTO_REFRESH_SECONDS = int(
    os.environ.get("AUTO_REFRESH_SECONDS", "60")
)

ANALYSIS_INTERVAL_SECONDS = 3600

# Bezpieczny start:
# 0 = bot nie wysyla zlecen
# 1 = bot moze handlowac na DEMO
ENABLE_DEMO_TRADING = (
    os.environ.get("ENABLE_DEMO_TRADING", "0") == "1"
)

CTRADER_CLIENT_ID = os.environ.get(
    "CTRADER_CLIENT_ID"
)

CTRADER_CLIENT_SECRET = os.environ.get(
    "CTRADER_CLIENT_SECRET"
)

CTRADER_REDIRECT_URI = os.environ.get(
    "CTRADER_REDIRECT_URI"
)

TELEGRAM_TOKEN = os.environ.get(
    "TELEGRAM_TOKEN", ""
)

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID", ""
)

WEBHOOK_SECRET = os.environ.get(
    "WEBHOOK_SECRET", ""
)


# ============================================================
# STATE DIRECTORY
# ============================================================

def choose_state_dir():

    configured = os.environ.get("BOT_STATE_DIR")

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
                path, ".write_test"
            )

            with open(probe, "w") as f:
                f.write("ok")

            os.remove(probe)

            return path

        except Exception:
            pass

    return tempfile.gettempdir()


BOT_STATE_DIR = choose_state_dir()

# Zachowujemy stary plik, aby nie utracic tokenow.
BOT_STATE_FILE = os.path.join(
    BOT_STATE_DIR,
    "us100_v10_state.json"
)


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
        "candles": {
            "M15": [],
            "H1": [],
        },
    }
}

pending_sl = {}
pending_close = {}

ctrader_client = None
reactor_started = False
refresh_started = False

state_lock = threading.RLock()


# ============================================================
# HELPERS
# ============================================================

def set_error(error):

    ctrader_state["error"] = str(error)

    print(
        "[ERROR]",
        error,
        flush=True,
    )


def clear_error():

    ctrader_state["error"] = None


def safe_errback(failure):

    set_error(str(failure))


def fmt_price(value):

    if value is None:
        return "Brak danych"

    try:
        return f"{float(value):,.2f}".replace(",", " ")

    except (ValueError, TypeError):
        return "Brak danych"


def fmt_money(value):

    if value is None:
        return "Brak danych"

    try:
        return f"{float(value):+,.2f}".replace(",", " ")

    except (ValueError, TypeError):
        return "Brak danych"


def normalize_symbol(name):

    return re.sub(
        r"[^A-Z0-9]",
        "",
        str(name or "").upper(),
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

    if any(x in value for x in aliases):
        return "US100"

    return None


def strip_accents(value):

    value = unicodedata.normalize(
        "NFKD",
        str(value),
    )

    return "".join(
        ch for ch in value
        if not unicodedata.combining(ch)
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_message(text):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:

        print(
            "[TELEGRAM DISABLED]",
            text,
            flush=True,
        )

        return False

    try:

        response = requests.post(
            f"https://api.telegram.org/bot"
            f"{TELEGRAM_TOKEN}/sendMessage",
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
            },
            timeout=15,
        )

        response.raise_for_status()

        return True

    except Exception as error:

        print(
            "[TELEGRAM ERROR]",
            error,
            flush=True,
        )

        return False


def telegram_signal(signal, emergency_sl, lots):

    side = signal["side"]

    message = (
        "📥 US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📡 SYGNAŁ TRADINGVIEW\n\n"
        f"{'🟢' if side == 'LONG' else '🔴'} "
        f"Kierunek: {side}\n"
        "📊 Interwał: M15\n\n"
        f"💰 Cena strategii: "
        f"{fmt_price(signal['strategy_entry'])}\n"
        f"🛑 SL strategii: "
        f"{fmt_price(signal['strategy_sl'])}\n"
        f"🎯 TP strategii: "
        f"{fmt_price(signal['strategy_tp'])}\n\n"
        f"🚨 Awaryjny SL: "
        f"{fmt_price(emergency_sl)}\n"
        f"📦 Wolumen: {lots:.2f} lot\n\n"
        "⏳ Zlecenie wysyłane do cTrader DEMO.\n"
        "ℹ️ TP jest poziomem informacyjnym."
    )

    send_telegram_message(message)


def telegram_position_opened(position, strategy_tp):

    side = position["side_name"]

    message = (
        "✅ US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📈 POZYCJA OTWARTA\n\n"
        f"{'🟢' if side == 'LONG' else '🔴'} "
        f"{side}\n\n"
        f"💰 Cena wejścia: "
        f"{fmt_price(position['price'])}\n"
        f"📦 Wolumen: "
        f"{FIXED_LOTS_US100:.2f} lot\n\n"
        f"🛑 Aktualny SL: "
        f"{fmt_price(position['stop_loss'])}\n"
        f"🎯 TP strategii: "
        f"{fmt_price(strategy_tp)}\n\n"
        "🤖 MANAGER REGRESJI\n"
        "📊 Faza: M15\n"
        "➡️ Po basis M15 przejście na H1.\n\n"
        "ℹ️ TP nie jest zleceniem w cTrader.\n"
        f"🆔 Pozycja: {position['position_id']}"
    )

    send_telegram_message(message)


def telegram_sl_changed(
    side,
    old_sl,
    new_sl,
    reason,
):

    message = (
        "🛡️ US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔄 STOP LOSS ZMIENIONY\n\n"
        f"📈 Kierunek: {side}\n\n"
        f"🔴 Poprzedni SL: "
        f"{fmt_price(old_sl)}\n"
        f"🟢 Nowy SL: "
        f"{fmt_price(new_sl)}\n\n"
        f"📊 Powód:\n{reason}\n\n"
        "✅ Zmiana potwierdzona przez cTrader."
    )

    send_telegram_message(message)


def telegram_basis_reached(side, basis, sl):

    message = (
        "🤖 US100 V11 MANAGER\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📊 ZMIANA FAZY\n\n"
        f"📈 Kierunek: {side}\n"
        "✅ Basis M15 osiągnięty\n\n"
        f"🎯 Basis: {fmt_price(basis)}\n"
        f"🛑 Ostatni znany SL: "
        f"{fmt_price(sl)}\n\n"
        "➡️ H1 PRZEJMUJE PROWADZENIE\n\n"
        "Manager obserwuje zamknięcia H1 "
        "i przesuwa SL po strukturze."
    )

    send_telegram_message(message)


def telegram_position_closed(
    side,
    entry,
    close_price,
    pnl,
    reason,
):

    message = (
        "🏁 US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔴 POZYCJA ZAMKNIĘTA\n\n"
        f"📈 Kierunek: {side}\n"
        f"💰 Wejście: {fmt_price(entry)}\n"
        f"💵 Zamknięcie: {fmt_price(close_price)}\n"
        f"💰 Wynik: {fmt_money(pnl)}\n\n"
        f"📋 Powód: {reason}\n\n"
        "✅ Zamknięcie potwierdzone."
    )

    send_telegram_message(message)


# ============================================================
# SAVE / LOAD
# ============================================================

def save_state():

    payload = {
        "access_token": ctrader_state["access_token"],
        "refresh_token": ctrader_state["refresh_token"],
        "last_signal_key": trade_state["last_signal_key"],
        "manager_state": manager_state,
    }

    temp = BOT_STATE_FILE + ".tmp"

    try:

        with state_lock:

            with open(
                temp,
                "w",
                encoding="utf-8",
            ) as handle:

                json.dump(
                    payload,
                    handle,
                    ensure_ascii=False,
                    indent=2,
                )

            os.replace(
                temp,
                BOT_STATE_FILE,
            )

        return True

    except Exception as error:

        set_error(error)

        return False


def load_state():

    if not os.path.exists(BOT_STATE_FILE):
        return False

    try:

        with state_lock:

            with open(
                BOT_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as handle:

                payload = json.load(handle)

        ctrader_state["access_token"] = payload.get(
            "access_token"
        )

        ctrader_state["refresh_token"] = payload.get(
            "refresh_token"
        )

        trade_state["last_signal_key"] = dict(
            payload.get("last_signal_key") or {}
        )

        manager_state.clear()

        manager_state.update(
            payload.get("manager_state") or {}
        )

        return True

    except Exception as error:

        set_error(error)

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

    return float(
        match.group(1).replace(",", ".")
    )


def parse_strategy_alert(text):

    raw = str(text or "").strip()

    normalized = re.sub(
        r"\s+",
        " ",
        strip_accents(raw).upper(),
    ).strip()

    if "WEJSCIE LONG" in normalized:
        side = "LONG"

    elif "WEJSCIE SHORT" in normalized:
        side = "SHORT"

    else:
        side = None

    symbol_match = re.search(
        r"SYMBOL\s*:\s*([^|]+)",
        normalized,
        re.I,
    )

    tf_match = re.search(
        r"TF\s*:\s*([^|]+)",
        normalized,
        re.I,
    )

    symbol_raw = (
        symbol_match.group(1).strip()
        if symbol_match else ""
    )

    timeframe = (
        tf_match.group(1).strip().lower()
        if tf_match else "?"
    )

    return {
        "event": "ENTRY" if side else "UNKNOWN",
        "side": side,
        "symbol": detect_instrument(symbol_raw),
        "timeframe": timeframe,
        "strategy_entry": extract_number(
            "CENA", normalized
        ),
        "strategy_sl": extract_number(
            "SL", normalized
        ),
        "strategy_tp": extract_number(
            "TP", normalized
        ),
        "raw": raw,
    }


# ============================================================
# VOLUME
# ============================================================

def fixed_volume_for_lots(lots):

    data = market_state["US100"]

    lot_size = int(
        data.get("lot_size_raw") or 0
    )

    minimum = int(
        data.get("min_volume_raw") or 0
    )

    maximum = int(
        data.get("max_volume_raw") or 0
    )

    step = int(
        data.get("step_volume_raw") or 0
    )

    if min(
        lot_size,
        minimum,
        maximum,
        step,
    ) <= 0:
        return None

    raw = int(
        round(float(lots) * lot_size)
    )

    if raw % step != 0:
        return None

    if not minimum <= raw <= maximum:
        return None

    return raw


def open_position_symbol_ids():

    return {
        int(p["symbol_id"])
        for p in ctrader_state["positions"]
    }


# ============================================================
# REGRESSION CHANNEL
# ============================================================

def closed_candles(timeframe):

    candles = list(
        market_state["US100"]["candles"].get(
            timeframe, []
        )
    )

    now = int(time.time())

    seconds = (
        900 if timeframe == "M15"
        else 3600
    )

    return [
        c for c in candles
        if int(c["timestamp"]) + seconds <= now
    ]


def regression_channel(candles):

    if len(candles) < REG_LENGTH:
        return None

    window = candles[-REG_LENGTH:]

    y = [
        float(c["close"])
        for c in window
    ]

    n = len(y)

    mean_x = (n - 1) / 2.0
    mean_y = sum(y) / n

    var_x = sum(
        (i - mean_x) ** 2
        for i in range(n)
    )

    if var_x <= 0:
        return None

    cov = sum(
        (i - mean_x) * (y[i] - mean_y)
        for i in range(n)
    )

    slope = cov / var_x

    intercept = (
        mean_y - slope * mean_x
    )

    fitted = [
        intercept + slope * i
        for i in range(n)
    ]

    residuals = [
        y[i] - fitted[i]
        for i in range(n)
    ]

    stdev = math.sqrt(
        sum(r * r for r in residuals) / n
    )

    basis = fitted[-1]

    return {
        "basis": basis,
        "upper": basis + REG_DEV * stdev,
        "lower": basis - REG_DEV * stdev,
        "slope": slope,
        "stdev": stdev,
        "bar_ts": int(
            window[-1]["timestamp"]
        ),
    }


def structure_stop(side, candles, bars):

    if len(candles) < bars:
        return None

    recent = candles[-bars:]

    if side == "LONG":

        return min(
            float(c["low"])
            for c in recent
        )

    return max(
        float(c["high"])
        for c in recent
    )


def is_tighter_sl(side, current, candidate):

    if candidate is None or candidate <= 0:
        return False

    if not current or current <= 0:
        return True

    if side == "LONG":
        return candidate > current

    return candidate < current


# ============================================================
# TELEGRAM ANALYSIS
# ============================================================

def send_position_analysis(position, state):

    m15 = closed_candles("M15")
    h1 = closed_candles("H1")

    m15_reg = regression_channel(m15)
    h1_reg = regression_channel(h1)

    if not m15_reg or not h1_reg:
        return

    # Ostatnia zamknieta swieca M15.
    # To nie jest aktualny tick Bid/Ask.
    price = float(m15[-1]["close"])

    side = state["side"]

    m15_basis = m15_reg["basis"]
    h1_basis = h1_reg["basis"]

    m15_position = (
        "POWYŻEJ"
        if price >= m15_basis
        else "PONIŻEJ"
    )

    h1_position = (
        "POWYŻEJ"
        if price >= h1_basis
        else "PONIŻEJ"
    )

    slope = h1_reg["slope"]

    if slope > 0:
        trend = "WZROSTOWY 📈"

    elif slope < 0:
        trend = "SPADKOWY 📉"

    else:
        trend = "BOCZNY ➡️"

    if (
        side == "LONG"
        and slope > 0
        and price >= h1_basis
    ):
        conclusion = (
            "Trend H1 wspiera LONG. "
            "Cena utrzymuje się nad basis."
        )

    elif (
        side == "SHORT"
        and slope < 0
        and price <= h1_basis
    ):
        conclusion = (
            "Trend H1 wspiera SHORT. "
            "Cena utrzymuje się pod basis."
        )

    else:
        conclusion = (
            "Uwaga: pozycja nie ma pełnego "
            "potwierdzenia z tunelu H1."
        )

    message = (
        "🧠 US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📊 ANALIZA POZYCJI\n\n"
        f"📈 Kierunek: {side}\n"
        f"💰 Wejście: "
        f"{fmt_price(state['entry'])}\n"
        f"💵 Ostatnie zamknięcie M15: "
        f"{fmt_price(price)}\n\n"
        f"🛑 Aktualny SL: "
        f"{fmt_price(position['stop_loss'])}\n"
        f"🎯 TP strategii: "
        f"{fmt_price(state.get('tv_tp_reference'))}\n\n"
        "📊 TUNEL REGRESJI\n\n"
        f"🔹 M15: {m15_position} basis\n"
        f"🔹 H1: {h1_position} basis\n"
        f"📈 Trend H1: {trend}\n"
        f"⚙️ Faza managera: {state['phase']}\n\n"
        f"🤖 OCENA:\n{conclusion}\n\n"
        "ℹ️ Analiza informacyjna. "
        "Nie zmienia zasad managera."
    )

    send_telegram_message(message)


# ============================================================
# POSITION CONTROL
# ============================================================

def amend_position_sl(position_id, new_sl, reason):

    if not ENABLE_DEMO_TRADING:
        return False

    if not ctrader_state["account_authorized"]:
        return False

    pid = str(position_id)

    if pid in pending_sl:
        return False

    position = next(
        (
            p for p in ctrader_state["positions"]
            if str(p["position_id"]) == pid
        ),
        None,
    )

    if position is None:
        return False

    old_sl = float(
        position.get("stop_loss") or 0
    )

    digits = (
        market_state["US100"]["digits"] or 2
    )

    new_sl = round(
        float(new_sl),
        digits,
    )

    side = position["side_name"]

    if not is_tighter_sl(
        side,
        old_sl,
        new_sl,
    ):
        return False

    req = ProtoOAAmendPositionSLTPReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.positionId = int(position_id)
    req.stopLoss = new_sl

    # Nie dodajemy ani nie zmieniamy TP.
    pending_sl[pid] = {
        "old": old_sl,
        "new": new_sl,
        "reason": reason,
        "side": side,
        "requested_at": time.time(),
    }

    print(
        "[SL REQUEST]",
        pid,
        old_sl,
        new_sl,
        flush=True,
    )

    def errback(failure):

        pending_sl.pop(pid, None)

        set_error(
            f"SL ERROR {pid}: {failure}"
        )

    ctrader_client.send(req).addErrback(
        errback
    )

    return True


def close_position(position_id, volume_raw, reason):

    if not ENABLE_DEMO_TRADING:
        return False

    if not ctrader_state["account_authorized"]:
        return False

    pid = str(position_id)

    if pid in pending_close:
        return False

    req = ProtoOAClosePositionReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.positionId = int(position_id)
    req.volume = int(volume_raw)

    pending_close[pid] = {
        "reason": reason,
        "requested_at": time.time(),
    }

    state = manager_state.get(pid)

    if state:
        state["close_requested"] = True

    def errback(failure):

        pending_close.pop(pid, None)

        if state:
            state["close_requested"] = False

        set_error(
            f"CLOSE ERROR {pid}: {failure}"
        )

    ctrader_client.send(req).addErrback(
        errback
    )

    send_telegram_message(
        "⏳ US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔴 Manager wysłał zlecenie zamknięcia.\n"
        f"📋 Powód: {reason}\n\n"
        "Oczekiwanie na potwierdzenie cTrader."
    )

    return True


# ============================================================
# MANAGER
# ============================================================

def ensure_manager_for_position(position):

    pid = str(position["position_id"])

    if pid in manager_state:
        return manager_state[pid]

    label = position.get("label") or ""

    last = trade_state.get("last_order") or {}

    last_pid = last.get("position_id")

    belongs = (
        label == BOT_LABEL
        or (
            last_pid is not None
            and int(last_pid) == int(pid)
        )
    )

    if not belongs:
        return None

    entry = float(
        position.get("price")
        or last.get("actual_entry")
        or last.get("entry")
        or 0
    )

    if entry <= 0:
        return None

    state = {
        "position_id": int(pid),
        "side": position["side_name"],
        "entry": entry,
        "tv_tp_reference": last.get(
            "strategy_tp"
        ),
        "phase": "M15",
        "basis_reached": False,
        "last_h1_bar": None,
        "last_analysis_at": 0,
        "close_requested": False,
        "opened_notified": False,
        "created_at": int(time.time()),
    }

    manager_state[pid] = state

    save_state()

    return state


def manage_position(position):

    state = ensure_manager_for_position(position)

    if not state:
        return

    pid = str(position["position_id"])

    if not state.get("opened_notified"):

        # Powiadomienie dopiero po pojawieniu sie
        # pozycji w danych cTrader.
        state["opened_notified"] = True

        telegram_position_opened(
            position,
            state.get("tv_tp_reference"),
        )

        save_state()

    if state.get("close_requested"):
        return

    side = state["side"]

    current_sl = float(
        position.get("stop_loss") or 0
    )

    m15 = closed_candles("M15")
    h1 = closed_candles("H1")

    if len(m15) < REG_LENGTH:
        return

    m15_reg = regression_channel(m15)

    if not m15_reg:
        return

    last_m15 = m15[-1]

    m15_ts = int(
        last_m15["timestamp"]
    )

    # =========================================
    # FAZA M15
    # =========================================

    if not state.get("basis_reached"):

        if side == "LONG":

            reached = (
                float(last_m15["high"])
                >= m15_reg["basis"]
            )

        else:

            reached = (
                float(last_m15["low"])
                <= m15_reg["basis"]
            )

        if reached:

            state["basis_reached"] = True
            state["phase"] = "H1"

            candidate = structure_stop(
                side,
                m15,
                M15_STRUCTURE_BARS,
            )

            if candidate is not None:

                close_price = float(
                    last_m15["close"]
                )

                valid = (
                    candidate < close_price
                    if side == "LONG"
                    else candidate > close_price
                )

                if (
                    valid
                    and is_tighter_sl(
                        side,
                        current_sl,
                        candidate,
                    )
                ):

                    amend_position_sl(
                        position["position_id"],
                        candidate,
                        "M15 basis osiągnięty - "
                        "SL po strukturze M15",
                    )

            telegram_basis_reached(
                side,
                m15_reg["basis"],
                current_sl,
            )

            save_state()

    # =========================================
    # ANALIZA CO GODZINE
    # =========================================

    now = time.time()

    if (
        now - state.get("last_analysis_at", 0)
        >= ANALYSIS_INTERVAL_SECONDS
    ):

        if regression_channel(h1):

            send_position_analysis(
                position,
                state,
            )

            state["last_analysis_at"] = now

            save_state()

    # =========================================
    # FAZA H1
    # =========================================

    if state["phase"] != "H1":
        return

    if len(h1) < REG_LENGTH:
        return

    h1_reg = regression_channel(h1)

    if not h1_reg:
        return

    last_h1 = h1[-1]

    h1_ts = int(
        last_h1["timestamp"]
    )

    if state.get("last_h1_bar") == h1_ts:
        return

    state["last_h1_bar"] = h1_ts

    h1_close = float(
        last_h1["close"]
    )

    invalidated = (
        h1_close < h1_reg["basis"]
        if side == "LONG"
        else h1_close > h1_reg["basis"]
    )

    if invalidated:

        close_position(
            position["position_id"],
            position["volume_raw"],
            "H1 zamknęła się po złej stronie "
            f"basis {fmt_price(h1_reg['basis'])}",
        )

        save_state()

        return

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

        if (
            valid
            and is_tighter_sl(
                side,
                current_sl,
                candidate,
            )
        ):

            amend_position_sl(
                position["position_id"],
                candidate,
                "H1 utrzymuje kierunek - "
                "SL po strukturze H1",
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

    for position in list(
        ctrader_state["positions"]
    ):

        if int(position["symbol_id"]) != int(
            symbol_id
        ):
            continue

        pid = str(
            position["position_id"]
        )

        open_ids.add(pid)

        manage_position(position)

    # Nie usuwamy od razu stanu przy zamknieciu.
    # Pozwala to zachowac dane do potwierdzenia
    # i powiadomienia o zamknieciu.


# ============================================================
# SEND ORDER
# ============================================================

def submit_strategy_market_order(signal):

    if not ENABLE_DEMO_TRADING:

        print(
            "[TV] DEMO TRADING DISABLED",
            flush=True,
        )

        send_telegram_message(
            "🔒 US100 V11 DEMO\n"
            "Sygnał odebrany, ale automatyczne "
            "zlecenia są wyłączone."
        )

        return

    if (
        ctrader_client is None
        or not ctrader_state["account_authorized"]
    ):
        return

    if "US100" in trade_state["pending_symbols"]:
        return

    symbol_id = market_state["US100"]["symbol_id"]

    if not symbol_id:
        return

    if int(symbol_id) in open_position_symbol_ids():
        return

    entry = signal["strategy_entry"]
    strategy_sl = signal["strategy_sl"]
    strategy_tp = signal["strategy_tp"]

    if signal["side"] == "LONG":

        if not strategy_sl < entry < strategy_tp:
            return

    else:

        if not strategy_tp < entry < strategy_sl:
            return

    signal_key = (
        f"{signal['side']}:"
        f"{signal['timeframe']}:"
        f"{entry:.5f}:"
        f"{strategy_sl:.5f}:"
        f"{strategy_tp:.5f}"
    )

    if (
        trade_state["last_signal_key"].get("US100")
        == signal_key
    ):
        return

    volume_raw = fixed_volume_for_lots(
        FIXED_LOTS_US100
    )

    if volume_raw is None:

        send_telegram_message(
            "⚠️ US100 V11 DEMO\n"
            "Nie mogę ustawić dokładnie "
            f"{FIXED_LOTS_US100:.2f} lot."
        )

        return

    if signal["side"] == "LONG":

        emergency_sl = (
            entry * (1 - EMERGENCY_SL_PCT)
        )

    else:

        emergency_sl = (
            entry * (1 + EMERGENCY_SL_PCT)
        )

    sl_distance = round(
        abs(entry - emergency_sl),
        2,
    )

    req = ProtoOANewOrderReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.symbolId = int(symbol_id)

    req.orderType = ProtoOAOrderType.MARKET

    req.tradeSide = (
        ProtoOATradeSide.BUY
        if signal["side"] == "LONG"
        else ProtoOATradeSide.SELL
    )

    req.volume = int(volume_raw)

    req.relativeStopLoss = max(
        1,
        int(round(sl_distance * 100000.0)),
    )

    # Bez relativeTakeProfit.
    req.label = BOT_LABEL

    req.comment = (
        f"US100 V11 M15 {signal['side']} "
        f"{FIXED_LOTS_US100:.2f} lot"
    )[:512]

    req.clientOrderId = (
        f"V11-{int(time.time())}-"
        f"{uuid.uuid4().hex[:8]}"
    )[:50]

    trade_state["pending_symbols"].add(
        "US100"
    )

    trade_state["last_signal_key"][
        "US100"
    ] = signal_key

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

    telegram_signal(
        signal,
        emergency_sl,
        FIXED_LOTS_US100,
    )

    def errback(failure):

        trade_state["pending_symbols"].discard(
            "US100"
        )

        trade_state["last_order_error"] = str(
            failure
        )

        set_error(failure)

    ctrader_client.send(req).addErrback(
        errback
    )


# ============================================================
# PROCESS ALERT
# ============================================================

def process_strategy_alert(text):

    signal = parse_strategy_alert(text)

    print(
        "[TV]",
        signal,
        flush=True,
    )

    if signal["event"] != "ENTRY":
        return

    if signal["side"] not in (
        "LONG",
        "SHORT",
    ):
        return

    if signal["symbol"] != "US100":
        return

    if signal["timeframe"] not in (
        "15",
        "15m",
        "15min",
        "m15",
    ):
        return

    if None in (
        signal["strategy_entry"],
        signal["strategy_sl"],
        signal["strategy_tp"],
    ):
        return

    if not reactor.running:
        return

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
        "timestamp": (
            int(bar.utcTimestampInMinutes) * 60
        ),
        "open": round(
            (low + int(bar.deltaOpen)) / 100000.0,
            digits,
        ),
        "high": round(
            (low + int(bar.deltaHigh)) / 100000.0,
            digits,
        ),
        "low": round(
            low / 100000.0,
            digits,
        ),
        "close": round(
            (low + int(bar.deltaClose)) / 100000.0,
            digits,
        ),
        "volume": int(bar.volume),
    }


def request_trendbars(client, timeframe):

    symbol_id = market_state["US100"]["symbol_id"]

    if not ctrader_state["account_authorized"]:
        return

    if not symbol_id:
        return

    now_ms = int(
        time.time() * 1000
    )

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

    req.fromTimestamp = (
        now_ms - 14 * 86400000
    )

    req.toTimestamp = now_ms
    req.count = 250

    client.send(req).addErrback(
        safe_errback
    )


# ============================================================
# ACCOUNT DATA
# ============================================================

def request_account_data(client):

    if not ctrader_state["account_authorized"]:
        return

    account_id = int(
        ctrader_state["account_id"]
    )

    if not market_state["US100"]["found"]:

        req = ProtoOASymbolsListReq()

        req.ctidTraderAccountId = account_id

        req.includeArchivedSymbols = False

        client.send(req).addErrback(
            safe_errback
        )

    for req in (
        ProtoOATraderReq(),
        ProtoOAReconcileReq(),
        ProtoOAGetPositionUnrealizedPnLReq(),
    ):

        req.ctidTraderAccountId = account_id

        client.send(req).addErrback(
            safe_errback
        )


# ============================================================
# REFRESH
# ============================================================

def refresh_market():

    if (
        ctrader_client is None
        or not ctrader_state["account_authorized"]
    ):
        return

    request_account_data(
        ctrader_client
    )

    if market_state["US100"]["digits"] is not None:

        request_trendbars(
            ctrader_client,
            "M15",
        )

        request_trendbars(
            ctrader_client,
            "H1",
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
            tick,
        )

    reactor.callLater(
        5.0,
        tick,
    )


# ============================================================
# AUTH
# ============================================================

def send_account_list(client):

    token = ctrader_state["access_token"]

    if not token:
        return

    req = ProtoOAGetAccountListByAccessTokenReq()

    req.accessToken = token

    client.send(req).addErrback(
        safe_errback
    )


def authorize_account(client):

    req = ProtoOAAccountAuthReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
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

    # TYLKO DEMO
    ctrader_client = Client(
        EndPoints.PROTOBUF_DEMO_HOST,
        EndPoints.PROTOBUF_PORT,
        TcpProtocol,
    )

    def connected(client):

        ctrader_state["connected"] = True

        clear_error()

        req = ProtoOAApplicationAuthReq()

        req.clientId = CTRADER_CLIENT_ID
        req.clientSecret = CTRADER_CLIENT_SECRET

        client.send(req).addErrback(
            safe_errback
        )

    def disconnected(client, reason):

        ctrader_state["connected"] = False

        ctrader_state[
            "account_authorized"
        ] = False

        set_error(
            f"DISCONNECTED: {reason}"
        )

    def on_message(client, message):

        payload_type = message.payloadType

        # ====================================
        # APPLICATION AUTH
        # ====================================

        if payload_type == (
            ProtoOAApplicationAuthRes().payloadType
        ):

            ctrader_state[
                "application_authorized"
            ] = True

            send_account_list(client)

        # ====================================
        # ACCOUNT LIST
        # ====================================

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
                        0,
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

            # TWARDY BLOK LIVE
            if bool(
                getattr(account, "isLive", False)
            ):

                set_error(
                    "LIVE BLOCKED"
                )

                return

            ctrader_state["account_id"] = int(
                account.ctidTraderAccountId
            )

            authorize_account(client)

        # ====================================
        # ACCOUNT AUTH
        # ====================================

        elif payload_type == (
            ProtoOAAccountAuthRes().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = True

            clear_error()

            request_account_data(client)

            start_refresh_loop()

        # ====================================
        # DISCONNECT
        # ====================================

        elif payload_type == (
            ProtoOAAccountDisconnectEvent().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = False

            reactor.callLater(
                1.0,
                authorize_account,
                client,
            )

        # ====================================
        # TOKEN INVALID
        # ====================================

        elif payload_type == (
            ProtoOAAccountsTokenInvalidatedEvent().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = False

            set_error(
                "Token wygasł. "
                "Zaloguj ponownie przez /ctrader/login"
            )

        # ====================================
        # TRADER
        # ====================================

        elif payload_type == (
            ProtoOATraderRes().payloadType
        ):

            response = Protobuf.extract(message)

            trader = response.trader

            divisor = 10 ** trader.moneyDigits

            ctrader_state["balance"] = round(
                trader.balance / divisor,
                2,
            )

        # ====================================
        # PNL
        # ====================================

        elif payload_type == (
            ProtoOAGetPositionUnrealizedPnLRes().payloadType
        ):

            response = Protobuf.extract(message)

            divisor = 10 ** response.moneyDigits

            pnl = sum(
                x.netUnrealizedPnL
                for x in response.positionUnrealizedPnL
            ) / divisor

            ctrader_state["unrealized_pnl"] = round(
                pnl,
                2,
            )

            if ctrader_state["balance"] is not None:

                ctrader_state["equity"] = round(
                    ctrader_state["balance"] + pnl,
                    2,
                )

        # ====================================
        # POSITIONS
        # ====================================

        elif payload_type == (
            ProtoOAReconcileRes().payloadType
        ):

            response = Protobuf.extract(message)

            positions = []

            for position in response.position:

                td = position.tradeData

                side = (
                    "LONG"
                    if int(td.tradeSide)
                    == int(ProtoOATradeSide.BUY)
                    else "SHORT"
                )

                positions.append({
                    "position_id": int(
                        position.positionId
                    ),
                    "symbol_id": int(
                        td.symbolId
                    ),
                    "volume_raw": int(
                        td.volume
                    ),
                    "side_name": side,
                    "label": str(
                        getattr(td, "label", "") or ""
                    ),
                    "price": float(
                        getattr(position, "price", 0) or 0
                    ),
                    "stop_loss": float(
                        getattr(position, "stopLoss", 0) or 0
                    ),
                    "take_profit": float(
                        getattr(position, "takeProfit", 0) or 0
                    ),
                })

            ctrader_state["positions"] = positions

            ctrader_state["orders"] = [
                int(order.orderId)
                for order in response.order
            ]

            # Potwierdzenie SL przez odczyt
            # aktualnego stanu pozycji.
            for position in positions:

                pid = str(
                    position["position_id"]
                )

                pending = pending_sl.get(pid)

                if not pending:
                    continue

                actual_sl = float(
                    position["stop_loss"]
                )

                requested_sl = float(
                    pending["new"]
                )

                digits = (
                    market_state["US100"]["digits"] or 2
                )

                tolerance = 10 ** (-digits)

                if abs(
                    actual_sl - requested_sl
                ) <= tolerance:

                    telegram_sl_changed(
                        pending["side"],
                        pending["old"],
                        actual_sl,
                        pending["reason"],
                    )

                    pending_sl.pop(
                        pid,
                        None,
                    )

            if ctrader_state["market_ready"]:

                reactor.callLater(
                    0.2,
                    run_trade_manager,
                )

        # ====================================
        # SYMBOL LIST
        # ====================================

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
                s for s in candidates
                if normalize_symbol(
                    s.symbolName
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

        # ====================================
        # SYMBOL DETAILS
        # ====================================

        elif payload_type == (
            ProtoOASymbolByIdRes().payloadType
        ):

            response = Protobuf.extract(message)

            for symbol in response.symbol:

                if int(symbol.symbolId) != int(
                    market_state["US100"]["symbol_id"]
                ):
                    continue

                data = market_state["US100"]

                data["digits"] = int(
                    symbol.digits
                )

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

                request_trendbars(
                    client,
                    "M15",
                )

                request_trendbars(
                    client,
                    "H1",
                )

        # ====================================
        # TREND BARS
        # ====================================

        elif payload_type == (
            ProtoOAGetTrendbarsRes().payloadType
        ):

            response = Protobuf.extract(message)

            if int(response.symbolId) != int(
                market_state["US100"]["symbol_id"]
            ):
                return

            timeframe = (
                "M15"
                if response.period
                == ProtoOATrendbarPeriod.M15
                else "H1"
                if response.period
                == ProtoOATrendbarPeriod.H1
                else None
            )

            if timeframe is None:
                return

            digits = (
                market_state["US100"]["digits"] or 2
            )

            candles = [
                trendbar_to_dict(
                    bar,
                    digits,
                )
                for bar in response.trendbar
            ]

            candles.sort(
                key=lambda x: x["timestamp"]
            )

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

            if ctrader_state["market_ready"]:

                reactor.callLater(
                    0.2,
                    run_trade_manager,
                )

        # ====================================
        # EXECUTION
        # ====================================

        elif payload_type == (
            ProtoOAExecutionEvent().payloadType
        ):

            response = Protobuf.extract(message)

            execution_type = int(
                response.executionType
            )

            if execution_type == int(
                ProtoOAExecutionType.ORDER_FILLED
            ):

                last = trade_state.get(
                    "last_order"
                ) or {}

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
                                0,
                            ) or 0
                        )

                    trade_state[
                        "pending_symbols"
                    ].discard("US100")

                    save_state()

                reactor.callLater(
                    0.5,
                    request_account_data,
                    client,
                )

            else:

                reactor.callLater(
                    0.5,
                    request_account_data,
                    client,
                )

        # ====================================
        # ORDER ERROR
        # ====================================

        elif payload_type == (
            ProtoOAOrderErrorEvent().payloadType
        ):

            response = Protobuf.extract(message)

            trade_state[
                "pending_symbols"
            ].clear()

            error_text = (
                f"{response.errorCode}: "
                f"{response.description}"
            )

            trade_state[
                "last_order_error"
            ] = error_text

            set_error(error_text)

            send_telegram_message(
                "⚠️ US100 V11 DEMO\n"
                "Błąd zlecenia cTrader:\n"
                f"{error_text}"
            )

        # ====================================
        # GENERIC ERROR
        # ====================================

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

                    request_account_data(
                        client
                    )

                else:

                    send_account_list(
                        client
                    )

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


# ============================================================
# BOOT
# ============================================================

def bootstrap():

    load_state()

    if not ctrader_state["access_token"]:

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

        if reactor.running:
            break

        time.sleep(0.1)

    if reactor.running:

        reactor.callFromThread(
            start_ctrader_connection
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "bot": "US100 V11 DEMO",
        "status": "ONLINE",
        "demo_only": True,
        "trading_enabled": ENABLE_DEMO_TRADING,
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
        "balance": ctrader_state["balance"],
        "equity": ctrader_state["equity"],
        "last_order": trade_state["last_order"],
        "manager_state": manager_state,
        "error": ctrader_state["error"],
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "version": "V11",
        "demo_only": True,
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
        "balance": ctrader_state["balance"],
        "equity": ctrader_state["equity"],
        "positions": ctrader_state["positions"],
        "last_error": ctrader_state["error"],
    })


@app.route("/trade/status")
def trade_status():

    return jsonify({
        "trading_enabled": ENABLE_DEMO_TRADING,
        "positions": ctrader_state["positions"],
        "last_order": trade_state["last_order"],
        "last_order_error": trade_state[
            "last_order_error"
        ],
        "manager_state": manager_state,
        "pending_sl": pending_sl,
        "pending_close": pending_close,
    })


@app.route("/telegram/test")
def telegram_test():

    ok = send_telegram_message(
        "🧪 US100 V11 DEMO\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "✅ Telegram działa poprawnie!\n\n"
        "📊 Instrument: US100\n"
        "📦 Wolumen: 2.00 lot\n"
        "🛑 Awaryjny SL: 0.70%\n"
        "🎯 TP: poziom strategii\n"
        "🤖 Manager: M15 → H1\n\n"
        "🔒 Konto DEMO: 5901967"
    )

    return jsonify({
        "status": (
            "success" if ok else "error"
        ),
        "telegram_sent": ok,
    })


@app.route("/webhook", methods=["POST"])
def webhook():

    # Sekret jest wymagany.
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
                or json.dumps(data)
            )

        else:

            text = str(data or "")

    else:

        text = request.get_data(
            as_text=True
        )

    if not text.strip():

        return jsonify({
            "status": "error",
            "message": "empty alert",
        }), 400

    threading.Thread(
        target=process_strategy_alert,
        args=(text,),
        daemon=True,
    ).start()

    return jsonify({
        "status": "accepted",
        "version": "V11",
        "demo_only": True,
    })


@app.route("/market/refresh")
def market_refresh():

    if not ctrader_state["account_authorized"]:

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

    if not ctrader_state["account_authorized"]:

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

    if not all((
        CTRADER_CLIENT_ID,
        CTRADER_CLIENT_SECRET,
        CTRADER_REDIRECT_URI,
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

        ctrader_state["access_token"] = (
            data.get("accessToken")
        )

        ctrader_state["refresh_token"] = (
            data.get("refreshToken")
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
        "version": "V11",
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
        os.environ.get("PORT", "10000")
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
