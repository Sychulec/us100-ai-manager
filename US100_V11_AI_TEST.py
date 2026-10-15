"""
============================================================
US100 V11 AI OBSERVER
DEMO ONLY - READ ONLY

TradingView -> Webhook -> Telegram
                     -> cTrader market data -> OpenAI -> Telegram

ZASADY:
- NIE OTWIERA TRANSAKCJI
- NIE ZAMYKA TRANSAKCJI
- NIE MODYFIKUJE SL / TP
- NIE ZMIENIA SYGNALOW TRADINGVIEW
- ENTRY / SL / TP ZAWSZE POCHODZA ZE STRATEGII
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
# CONFIGURATION
# ============================================================

BOT_NAME = "US100_V11_AI_OBSERVER"

TARGET_TRADER_LOGIN = 5901967

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

AI_MODEL = os.getenv(
    "OPENAI_MODEL",
    "gpt-5"
)

OPENAI_API_KEY = os.getenv(
    "OPENAI_API_KEY",
    ""
)

CTRADER_CLIENT_ID = os.getenv(
    "CTRADER_CLIENT_ID",
    ""
)

CTRADER_CLIENT_SECRET = os.getenv(
    "CTRADER_CLIENT_SECRET",
    ""
)

CTRADER_REDIRECT_URI = os.getenv(
    "CTRADER_REDIRECT_URI",
    ""
)

TELEGRAM_TOKEN = os.getenv(
    "TELEGRAM_TOKEN",
    ""
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
)

WEBHOOK_SECRET = os.getenv(
    "WEBHOOK_SECRET",
    ""
)


# ============================================================
# STATE DIRECTORY
# ============================================================

def choose_state_dir():

    candidates = (
        [os.getenv("BOT_STATE_DIR")]
        if os.getenv("BOT_STATE_DIR")
        else ["/var/data", "/tmp/us100-v11-ai"]
    )

    for path in candidates:

        try:

            os.makedirs(
                path,
                exist_ok=True
            )

            probe = os.path.join(
                path,
                ".write_test"
            )

            with open(probe, "w") as f:
                f.write("ok")

            os.remove(probe)

            return path

        except Exception:
            pass

    return tempfile.gettempdir()


BOT_STATE_DIR = choose_state_dir()

BOT_STATE_FILE = os.path.join(
    BOT_STATE_DIR,
    "us100_v11_ai_state.json"
)

AI_LOG_FILE = os.path.join(
    BOT_STATE_DIR,
    "us100_v11_ai_decisions.jsonl"
)


# ============================================================
# STATES
# ============================================================

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

    "ai_calls": 0,

    "ai_errors": 0,

    "last_signal": None,

    "last_ai_decision": None,

    "last_telegram_sent": None,

}


ctrader_client = None

reactor_started = False

refresh_started = False

state_lock = threading.RLock()

market_lock = threading.RLock()


# ============================================================
# ERROR HANDLING
# ============================================================

def set_error(error):

    ctrader_state["error"] = str(error)

    print(
        "[ERROR]",
        error,
        flush=True
    )


def safe_errback(failure):

    set_error(str(failure))


# ============================================================
# SYMBOL
# ============================================================

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

    return (
        "US100"
        if any(alias in value for alias in aliases)
        else None
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_message(text):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:

        print(
            "[TELEGRAM DISABLED]",
            text,
            flush=True
        )

        return False

    try:

        url = (
            "https://api.telegram.org/bot"
            + TELEGRAM_TOKEN
            + "/sendMessage"
        )

        response = requests.post(

            url,

            data={

                "chat_id": TELEGRAM_CHAT_ID,

                "text": text,

            },

            timeout=15

        )

        response.raise_for_status()

        result = bool(
            response.json().get("ok")
        )

        if result:

            observer_state["last_telegram_sent"] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

        return result

    except Exception as exc:

        print(
            "[TELEGRAM ERROR]",
            exc,
            flush=True
        )

        return False


# ============================================================
# SAVE / LOAD STATE
# ============================================================

def save_state():

    payload = {

        "access_token":
            ctrader_state["access_token"],

        "refresh_token":
            ctrader_state["refresh_token"],

        "observer_state":
            observer_state,

    }

    try:

        with state_lock:

            temp = BOT_STATE_FILE + ".tmp"

            with open(
                temp,
                "w",
                encoding="utf-8"
            ) as f:

                json.dump(
                    payload,
                    f,
                    ensure_ascii=False,
                    indent=2
                )

            os.replace(
                temp,
                BOT_STATE_FILE
            )

        return True

    except Exception as exc:

        print(
            "[STATE ERROR]",
            exc,
            flush=True
        )

        return False


def load_state():

    try:

        with state_lock:

            with open(
                BOT_STATE_FILE,
                encoding="utf-8"
            ) as f:

                payload = json.load(f)

        ctrader_state["access_token"] = (
            payload.get("access_token")
        )

        ctrader_state["refresh_token"] = (
            payload.get("refresh_token")
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

    except Exception as exc:

        print(
            "[STATE LOAD ERROR]",
            exc,
            flush=True
        )

        return False


# ============================================================
# ALERT PARSER
# ============================================================

def strip_accents(text):

    return "".join(

        char

        for char in unicodedata.normalize(
            "NFKD",
            str(text)
        )

        if not unicodedata.combining(char)

    )


def extract_number(label, text):

    pattern = (
        rf"\b{label}\s*:\s*"
        r"(-?[0-9]+(?:[.,][0-9]+)?)"
    )

    match = re.search(
        pattern,
        text,
        re.I
    )

    if not match:

        return None

    try:

        value = float(
            match.group(1).replace(
                ",",
                "."
            )
        )

        if not math.isfinite(value):
            return None

        return value

    except Exception:

        return None


def parse_strategy_alert(text):

    raw = str(text or "").strip()

    normalized = re.sub(

        r"\s+",

        " ",

        strip_accents(
            raw
        ).upper()

    ).strip()

    side = None

    if "WEJSCIE LONG" in normalized:

        side = "LONG"

    elif "WEJSCIE SHORT" in normalized:

        side = "SHORT"

    symbol_match = re.search(

        r"\bSYMBOL\s*:\s*([^|]+)",

        normalized

    )

    tf_match = re.search(

        r"\bTF\s*:\s*([^|]+)",

        normalized

    )

    symbol_raw = (
        symbol_match.group(1).strip()
        if symbol_match
        else ""
    )

    timeframe = (
        tf_match.group(1).strip().lower()
        if tf_match
        else "?"
    )

    return {

        "event":
            "ENTRY" if side else "UNKNOWN",

        "side":
            side,

        "symbol":
            detect_instrument(symbol_raw),

        "timeframe":
            timeframe,

        "strategy_entry":
            extract_number(
                "CENA",
                normalized
            ),

        "strategy_sl":
            extract_number(
                "SL",
                normalized
            ),

        "strategy_tp":
            extract_number(
                "TP",
                normalized
            ),

        "raw":
            raw,

    }


# ============================================================
# FORMAT LEVEL
# ============================================================

def format_level(value):

    if value is None:

        return "BRAK W ALERCIE"

    try:

        number = float(value)

        if not math.isfinite(number):

            return "BRAK W ALERCIE"

        return f"{number:.2f}"

    except Exception:

        return "BRAK W ALERCIE"


# ============================================================
# TRADINGVIEW -> TELEGRAM
# ============================================================

def send_tradingview_signal(signal):

    side = signal["side"]

    direction_icon = (
        "🟢"
        if side == "LONG"
        else "🔴"
    )

    message = (

        "📊 US100 V11 AI OBSERVER\n\n"

        "📡 SYGNAŁ TRADINGVIEW\n\n"

        f"{direction_icon} Kierunek: {side}\n"

        f"💰 ENTRY: "
        f"{format_level(signal['strategy_entry'])}\n"

        f"🛑 SL: "
        f"{format_level(signal['strategy_sl'])}\n"

        f"🎯 TP: "
        f"{format_level(signal['strategy_tp'])}\n\n"

        f"⏱ TF: {signal['timeframe']}\n\n"

        "🤖 AI: analiza w osobnej wiadomości\n"

        "🔒 TYLKO OBSERWACJA - BRAK TRANSAKCJI"

    )

    return send_telegram_message(
        message
    )


# ============================================================
# CTRADER TREND BARS
# ============================================================

def trendbar_to_dict(bar, digits):

    low = int(bar.low)

    return {

        "timestamp":
            int(
                bar.utcTimestampInMinutes
            ) * 60,

        "open":
            round(
                (
                    low
                    + int(bar.deltaOpen)
                ) / 100000.0,
                digits
            ),

        "high":
            round(
                (
                    low
                    + int(bar.deltaHigh)
                ) / 100000.0,
                digits
            ),

        "low":
            round(
                low / 100000.0,
                digits
            ),

        "close":
            round(
                (
                    low
                    + int(bar.deltaClose)
                ) / 100000.0,
                digits
            ),

        "volume":
            int(bar.volume),

    }


def closed_candles(tf):

    with market_lock:

        candles = list(
            market_state["US100"]["candles"].get(
                tf,
                []
            )
        )

    seconds = (
        900
        if tf == "M15"
        else 3600
    )

    now = int(time.time())

    return [

        candle

        for candle in candles

        if (
            candle["timestamp"]
            + seconds
        ) <= now

    ]


# ============================================================
# REGRESSION CHANNEL
# ============================================================

def regression_channel(candles, length=None):

    length = (
        length
        if length is not None
        else REG_LENGTH
    )

    if len(candles) < length:

        return None

    window = candles[-length:]

    values = [
        float(c["close"])
        for c in window
    ]

    n = len(values)

    mx = (n - 1) / 2

    my = sum(values) / n

    vx = sum(

        (i - mx) ** 2

        for i in range(n)

    )

    if vx <= 0:

        return None

    slope = sum(

        (i - mx)
        * (values[i] - my)

        for i in range(n)

    ) / vx

    intercept = my - slope * mx

    residuals = [

        values[i]
        - (
            intercept
            + slope * i
        )

        for i in range(n)

    ]

    stdev = math.sqrt(

        sum(
            r * r
            for r in residuals
        ) / n

    )

    basis = (
        intercept
        + slope * (n - 1)
    )

    return {

        "basis":
            basis,

        "upper":
            basis + REG_DEV * stdev,

        "lower":
            basis - REG_DEV * stdev,

        "slope":
            slope,

        "stdev":
            stdev,

        "width":
            2 * REG_DEV * stdev,

        "bar_ts":
            int(
                window[-1]["timestamp"]
            ),

    }


def monthly_regression(candles):

    now = datetime.now(
        timezone.utc
    )

    monthly = []

    for candle in candles:

        candle_date = datetime.fromtimestamp(

            int(
                candle["timestamp"]
            ),

            timezone.utc

        )

        if (
            candle_date.year == now.year
            and candle_date.month == now.month
        ):

            monthly.append(
                candle
            )

    if len(monthly) < 10:

        return None

    return regression_channel(
        monthly,
        len(monthly)
    )


# ============================================================
# CHANNEL POSITION
# ============================================================

def channel_position(price, channel):

    if channel is None or price is None:

        return None

    upper = channel["upper"]

    lower = channel["lower"]

    basis = channel["basis"]

    if upper <= lower:

        return None

    if price > upper:

        zone = "ABOVE_UPPER"

    elif price < lower:

        zone = "BELOW_LOWER"

    elif price > basis:

        zone = "UPPER_HALF"

    elif price < basis:

        zone = "LOWER_HALF"

    else:

        zone = "BASIS"

    return {

        "zone":
            zone,

        "position_0_1":
            round(
                (price - lower)
                / (upper - lower),
                4
            ),

        "distance_to_basis":
            round(
                price - basis,
                4
            ),

    }


# ============================================================
# MARKET STRUCTURE
# ============================================================

def structure_summary(candles):

    if len(candles) < 12:

        return {
            "structure": "UNKNOWN"
        }

    first = candles[-12:-6]

    second = candles[-6:]

    first_high = max(
        c["high"]
        for c in first
    )

    second_high = max(
        c["high"]
        for c in second
    )

    first_low = min(
        c["low"]
        for c in first
    )

    second_low = min(
        c["low"]
        for c in second
    )

    if (
        second_high > first_high
        and second_low > first_low
    ):

        structure = "HH_HL"

    elif (
        second_high < first_high
        and second_low < first_low
    ):

        structure = "LH_LL"

    else:

        structure = "MIXED"

    return {

        "structure":
            structure,

        "first_high":
            first_high,

        "second_high":
            second_high,

        "first_low":
            first_low,

        "second_low":
            second_low,

    }


# ============================================================
# BAND WALK
# ============================================================

def band_walk_summary(candles):

    if len(candles) < REG_LENGTH + 8:

        return {
            "status": "UNKNOWN"
        }

    upper_count = 0

    lower_count = 0

    for i in range(
        len(candles) - 8,
        len(candles)
    ):

        channel = regression_channel(
            candles[:i + 1]
        )

        if not channel:

            continue

        if (
            candles[i]["close"]
            > channel["basis"]
        ):

            upper_count += 1

        if (
            candles[i]["close"]
            < channel["basis"]
        ):

            lower_count += 1

    if upper_count >= 6:

        status = "WALKING_UPPER_HALF"

    elif lower_count >= 6:

        status = "WALKING_LOWER_HALF"

    else:

        status = "BALANCED"

    return {

        "status":
            status,

        "upper_half_bars":
            upper_count,

        "lower_half_bars":
            lower_count,

    }


# ============================================================
# VOLUME
# ============================================================

def volume_summary(candles):

    if len(candles) < 21:

        return {
            "status": "UNKNOWN"
        }

    average = sum(

        c["volume"]

        for c in candles[-21:-1]

    ) / 20

    last = candles[-1]["volume"]

    ratio = (
        last / average
        if average
        else 0
    )

    if ratio >= 1.5:

        status = "VERY_HIGH"

    elif ratio >= 1.15:

        status = "HIGH"

    elif ratio <= 0.7:

        status = "LOW"

    else:

        status = "NORMAL"

    return {

        "status":
            status,

        "last_volume":
            last,

        "average_20":
            round(
                average,
                2
            ),

        "ratio":
            round(
                ratio,
                3
            ),

    }


# ============================================================
# MARKET SNAPSHOT
# ============================================================

def build_market_snapshot(signal):

    m15 = closed_candles("M15")

    h1 = closed_candles("H1")

    if (
        len(m15) < REG_LENGTH
        or len(h1) < REG_LENGTH
    ):

        return None

    m15_reg = regression_channel(
        m15
    )

    h1_reg = regression_channel(
        h1
    )

    if not m15_reg or not h1_reg:

        return None

    monthly = monthly_regression(
        m15
    )

    price = float(
        m15[-1]["close"]
    )

    def block(
        candles,
        regression,
        current_price,
        recent
    ):

        return {

            "last_closed":
                candles[-1],

            "regression":
                regression,

            "location":
                channel_position(
                    current_price,
                    regression
                ),

            "structure":
                structure_summary(
                    candles
                ),

            "band_walk":
                band_walk_summary(
                    candles
                ),

            "volume":
                volume_summary(
                    candles
                ),

            "recent_candles":
                candles[-recent:],

        }

    return {

        "instrument":
            "US100",

        "observer_version":
            "V11",

        "signal_time_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "signal": {

            "side":
                signal["side"],

            "entry":
                signal["strategy_entry"],

            "strategy_sl":
                signal["strategy_sl"],

            "strategy_tp":
                signal["strategy_tp"],

            "timeframe":
                signal["timeframe"],

        },

        "market_price":
            price,

        "M15":
            block(
                m15,
                m15_reg,
                price,
                20
            ),

        "H1":
            block(
                h1,
                h1_reg,
                float(h1[-1]["close"]),
                12
            ),

        "MONTH": (

            {

                "regression":
                    monthly,

                "location":
                    channel_position(
                        price,
                        monthly
                    ),

            }

            if monthly

            else None

        ),

    }


# ============================================================
# AI INSTRUCTIONS
# ============================================================

AI_INSTRUCTIONS = """
Jestes filtrem wejsc strategii regresji US100.

TYLKO OBSERWACJA.

Nie wydajesz zlecen.
Nie otwierasz pozycji.
Nie zamykasz pozycji.
Nie zmieniasz ENTRY, SL ani TP.

Strategia M15 wykorzystuje tunel regresji.

LONG po powrocie nad dolna bande.
SHORT po powrocie pod gorna bande.

Szczegolnie wykrywaj:
- wejscia kontrtrendowe,
- silny trend spadkowy,
- silny trend wzrostowy,
- band walking,
- strukture HH/HL,
- strukture LH/LL,
- nachylenie tunelu,
- polozenie ceny wobec srodka tunelu,
- zgodnosc M15 i H1,
- kontekst miesieczny,
- wolumen.

Nie odrzucaj sygnalu tylko dlatego,
ze pojawil sie przy bandzie tunelu.

Nie twierdz, ze znasz przyszlosc.

market_regime:
TREND_WZROSTOWY
TREND_SPADKOWY
KONSOLIDACJA
PRZEJSCIE
NIEJASNY

decision:
AKCEPTUJ
ODRZUC
OBSERWUJ

score:
0-100

Podaj krotki konkretny powod.
"""


# ============================================================
# OPENAI
# ============================================================

def call_openai(snapshot):

    if not OPENAI_API_KEY:

        raise RuntimeError(
            "Brak OPENAI_API_KEY"
        )

    schema = {

        "type":
            "object",

        "properties": {

            "decision": {

                "type":
                    "string",

                "enum": [
                    "AKCEPTUJ",
                    "ODRZUC",
                    "OBSERWUJ",
                ],

            },

            "score": {

                "type":
                    "integer",

                "minimum":
                    0,

                "maximum":
                    100,

            },

            "market_regime": {

                "type":
                    "string",

                "enum": [
                    "TREND_WZROSTOWY",
                    "TREND_SPADKOWY",
                    "KONSOLIDACJA",
                    "PRZEJSCIE",
                    "NIEJASNY",
                ],

            },

            "signal_alignment": {

                "type":
                    "string",

                "enum": [
                    "ZGODNY_Z_RUCHEM",
                    "PRZECIW_RUCHOWI",
                    "NEUTRALNY",
                ],

            },

            "reason": {

                "type":
                    "string",

            },

        },

        "required": [
            "decision",
            "score",
            "market_regime",
            "signal_alignment",
            "reason",
        ],

        "additionalProperties":
            False,

    }

    payload = {

        "model":
            AI_MODEL,

        "instructions":
            AI_INSTRUCTIONS,

        "input":
            (
                "Ocen ponizszy sygnal US100:\n"
                + json.dumps(
                    snapshot,
                    ensure_ascii=False,
                    separators=(",", ":")
                )
            ),

        "text": {

            "format": {

                "type":
                    "json_schema",

                "name":
                    "us100_signal_evaluation",

                "strict":
                    True,

                "schema":
                    schema,

            }

        },

        "max_output_tokens":
            500,

    }

    response = requests.post(

        "https://api.openai.com/v1/responses",

        headers={

            "Authorization":
                f"Bearer {OPENAI_API_KEY}",

            "Content-Type":
                "application/json",

        },

        json=payload,

        timeout=45

    )

    if not response.ok:

        raise RuntimeError(

            f"OpenAI HTTP "
            f"{response.status_code}: "
            f"{response.text[:500]}"

        )

    output = "".join(

        content.get(
            "text",
            ""
        )

        for item in response.json().get(
            "output",
            []
        )

        if item.get("type") == "message"

        for content in item.get(
            "content",
            []
        )

        if content.get("type") == "output_text"

    )

    if not output:

        raise RuntimeError(
            "OpenAI zwrocil pusty output"
        )

    return json.loads(
        output
    )


# ============================================================
# AI LOG
# ============================================================

def save_ai_decision(
    signal,
    snapshot,
    result
):

    record = {

        "timestamp":
            int(time.time()),

        "time_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "signal":
            signal,

        "snapshot":
            snapshot,

        "ai":
            result,

    }

    try:

        with state_lock:

            with open(
                AI_LOG_FILE,
                "a",
                encoding="utf-8"
            ) as f:

                f.write(

                    json.dumps(
                        record,
                        ensure_ascii=False
                    )

                    + "\n"

                )

    except Exception as exc:

        print(
            "[AI LOG ERROR]",
            exc,
            flush=True
        )


# ============================================================
# AI ANALYSIS
# ============================================================

def analyse_signal_with_ai(signal):

    try:

        snapshot = build_market_snapshot(
            signal
        )

        if snapshot is None:

            m15_count = len(
                closed_candles("M15")
            )

            h1_count = len(
                closed_candles("H1")
            )

            send_telegram_message(

                "🤖 US100 V11 AI OBSERVER\n\n"

                f"Sygnał: {signal['side']}\n"

                "AI: ANALIZA NIEDOSTĘPNA\n\n"

                "Powód: dane M15/H1 "
                "nie są jeszcze gotowe.\n\n"

                f"M15: {m15_count} świec\n"

                f"H1: {h1_count} świec\n\n"

                f"Wymagane: {REG_LENGTH} "
                "świec na każdym TF.\n\n"

                "🔒 TYLKO OBSERWACJA"

            )

            return

        observer_state["ai_calls"] += 1

        result = call_openai(
            snapshot
        )

        observer_state["last_ai_decision"] = (
            result
        )

        save_state()

        save_ai_decision(
            signal,
            snapshot,
            result
        )

        message = (

            "🤖 US100 V11 AI OBSERVER\n\n"

            f"📡 TradingView: {signal['side']}\n"

            f"💰 ENTRY: "
            f"{format_level(signal['strategy_entry'])}\n"

            f"🛑 SL: "
            f"{format_level(signal['strategy_sl'])}\n"

            f"🎯 TP: "
            f"{format_level(signal['strategy_tp'])}\n\n"

            f"AI: {result['decision']}\n"

            f"Ocena: {result['score']}/100\n"

            f"Rynek: {result['market_regime']}\n"

            f"Układ: {result['signal_alignment']}\n\n"

            f"Powód: {result['reason']}\n\n"

            "🔒 TYLKO OBSERWACJA - BRAK TRANSAKCJI"

        )

        send_telegram_message(
            message
        )

    except Exception as exc:

        observer_state["ai_errors"] += 1

        observer_state["last_ai_decision"] = {

            "error":
                str(exc)

        }

        save_state()

        print(
            "[AI ERROR]",
            exc,
            flush=True
        )

        send_telegram_message(

            "⚠️ US100 V11 AI OBSERVER\n\n"

            "AI ERROR:\n"

            f"{str(exc)[:300]}\n\n"

            "ENTRY / SL / TP "
            "wysłano w osobnej wiadomości."

        )


# ============================================================
# PROCESS TRADINGVIEW ALERT
# ============================================================

def process_strategy_alert(text):

    print(

        "[TV] RAW:",

        str(text).replace(
            "\n",
            " | "
        )[:1000],

        flush=True

    )

    signal = parse_strategy_alert(
        text
    )

    print(
        "[TV] PARSED:",
        signal,
        flush=True
    )

    if (
        signal["event"] != "ENTRY"
        or signal["side"] not in (
            "LONG",
            "SHORT"
        )
    ):

        print(
            "[TV] REJECTED: event/side",
            flush=True
        )

        return

    if signal["symbol"] != "US100":

        print(
            "[TV] REJECTED: symbol",
            flush=True
        )

        return

    if signal["timeframe"] not in (
        "15",
        "15m",
        "15min",
        "m15"
    ):

        print(
            "[TV] REJECTED: timeframe",
            flush=True
        )

        return

    observer_state["signals_received"] += 1

    observer_state["last_signal"] = signal

    save_state()

    # 1. FIRST - SEND STRATEGY LEVELS

    delivered = send_tradingview_signal(
        signal
    )

    print(
        "[TV] ENTRY/SL/TP TELEGRAM:",
        delivered,
        flush=True
    )

    # 2. SECOND - AI ANALYSIS

    levels = (

        signal["strategy_entry"],

        signal["strategy_sl"],

        signal["strategy_tp"],

    )

    valid_levels = all(

        value is not None
        and math.isfinite(value)

        for value in levels

    )

    if not valid_levels:

        send_telegram_message(

            "⚠️ US100 V11\n\n"

            "Niekompletne dane "
            "ENTRY / SL / TP "
            "z TradingView.\n\n"

            "Analiza AI pominięta.\n"

            "🔒 BRAK TRANSAKCJI"

        )

        return

    analyse_signal_with_ai(
        signal
    )


# ============================================================
# CTRADER REQUESTS
# ============================================================

def request_trendbars(client, tf):

    symbol_id = market_state["US100"]["symbol_id"]

    if (
        not ctrader_state["account_authorized"]
        or not symbol_id
    ):

        return

    req = ProtoOAGetTrendbarsReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.symbolId = int(
        symbol_id
    )

    req.period = (

        ProtoOATrendbarPeriod.M15

        if tf == "M15"

        else ProtoOATrendbarPeriod.H1

    )

    now_ms = int(
        time.time() * 1000
    )

    days = (
        35
        if tf == "M15"
        else 30
    )

    req.fromTimestamp = (
        now_ms - days * 86400000
    )

    req.toTimestamp = now_ms

    req.count = 1500

    client.send(
        req
    ).addErrback(
        safe_errback
    )


def request_account_data(client):

    if (
        ctrader_state["account_authorized"]
        and not market_state["US100"]["found"]
    ):

        req = ProtoOASymbolsListReq()

        req.ctidTraderAccountId = int(
            ctrader_state["account_id"]
        )

        req.includeArchivedSymbols = False

        client.send(
            req
        ).addErrback(
            safe_errback
        )


def refresh_market():

    if (
        ctrader_client is None
        or not ctrader_state["account_authorized"]
    ):

        return

    if market_state["US100"]["digits"] is not None:

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

    token = ctrader_state.get(
        "access_token"
    )

    if not token:

        set_error(
            "Brak access_token"
        )

        return

    req = ProtoOAGetAccountListByAccessTokenReq()

    req.accessToken = token

    client.send(
        req
    ).addErrback(
        safe_errback
    )


def authorize_account(client):

    if not ctrader_state.get(
        "account_id"
    ):

        return

    req = ProtoOAAccountAuthReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.accessToken = ctrader_state[
        "access_token"
    ]

    client.send(
        req
    ).addErrback(
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

        client.send(
            req
        ).addErrback(
            safe_errback
        )

    def disconnected(client, reason):

        ctrader_state["connected"] = False

        ctrader_state["account_authorized"] = False

        print(
            "[CTRADER] DISCONNECTED",
            reason,
            flush=True
        )

    def on_message(client, message):

        pt = message.payloadType

        if pt == ProtoOAApplicationAuthRes().payloadType:

            ctrader_state["application_authorized"] = True

            send_account_list(
                client
            )

        elif pt == ProtoOAGetAccountListByAccessTokenRes().payloadType:

            response = Protobuf.extract(
                message
            )

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

                ctrader_state["account_authorized"] = False

                set_error(
                    "LIVE BLOCKED - DEMO ONLY"
                )

                return

            ctrader_state["account_id"] = int(
                account.ctidTraderAccountId
            )

            authorize_account(
                client
            )

        elif pt == ProtoOAAccountAuthRes().payloadType:

            ctrader_state["account_authorized"] = True

            ctrader_state["error"] = None

            request_account_data(
                client
            )

            start_refresh_loop()

        elif pt == ProtoOAAccountDisconnectEvent().payloadType:

            ctrader_state["account_authorized"] = False

            reactor.callLater(
                1.0,
                authorize_account,
                client
            )

        elif pt == ProtoOAAccountsTokenInvalidatedEvent().payloadType:

            ctrader_state["account_authorized"] = False

            set_error(
                "Token cTrader wygasł - /ctrader/login"
            )

        elif pt == ProtoOASymbolsListRes().payloadType:

            response = Protobuf.extract(
                message
            )

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
                exact or candidates
            )[0]

            market_state["US100"].update({

                "found":
                    True,

                "symbol_id":
                    int(
                        symbol.symbolId
                    ),

                "symbol_name":
                    symbol.symbolName,

            })

            req = ProtoOASymbolByIdReq()

            req.ctidTraderAccountId = int(
                ctrader_state["account_id"]
            )

            req.symbolId.append(
                int(
                    symbol.symbolId
                )
            )

            client.send(
                req
            ).addErrback(
                safe_errback
            )

        elif pt == ProtoOASymbolByIdRes().payloadType:

            response = Protobuf.extract(
                message
            )

            for symbol in response.symbol:

                if int(
                    symbol.symbolId
                ) != int(
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

        elif pt == ProtoOAGetTrendbarsRes().payloadType:

            response = Protobuf.extract(
                message
            )

            if int(
                response.symbolId
            ) != int(
                market_state["US100"]["symbol_id"] or 0
            ):

                return

            if response.period == ProtoOATrendbarPeriod.M15:

                tf = "M15"

            elif response.period == ProtoOATrendbarPeriod.H1:

                tf = "H1"

            else:

                return

            digits = market_state["US100"]["digits"]

            if digits is None:

                return

            candles = sorted(

                (

                    trendbar_to_dict(
                        bar,
                        digits
                    )

                    for bar in response.trendbar

                ),

                key=lambda c: c["timestamp"]

            )

            with market_lock:

                market_state["US100"]["candles"][tf] = (
                    candles
                )

                ctrader_state["market_ready"] = all(

                    len(
                        market_state["US100"]["candles"][t]
                    ) >= REG_LENGTH

                    for t in (
                        "M15",
                        "H1"
                    )

                )

                ctrader_state["last_market_refresh"] = (
                    int(time.time())
                )

        elif pt == ProtoOAErrorRes().payloadType:

            response = Protobuf.extract(
                message
            )

            code = str(
                response.errorCode
            )

            desc = str(
                response.description
            )

            if "ALREADY_LOGGED_IN" in (
                code + desc
            ):

                if ctrader_state.get(
                    "account_id"
                ):

                    ctrader_state["account_authorized"] = True

                    request_account_data(
                        client
                    )

                else:

                    ctrader_state["application_authorized"] = True

                    send_account_list(
                        client
                    )

                return

            set_error(
                f"{code}: {desc}"
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

        if getattr(
            reactor,
            "running",
            False
        ):

            return True

        time.sleep(
            0.1
        )

    return False


# ============================================================
# BOOTSTRAP
# ============================================================

def bootstrap():

    load_state()

    if not ctrader_state.get(
        "access_token"
    ):

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

    return jsonify(

        bot=BOT_NAME,

        status="ONLINE",

        mode="AI_OBSERVER_ONLY",

        trading_enabled=False,

        demo_only=True,

        target_trader_login=TARGET_TRADER_LOGIN,

        ai_model=AI_MODEL,

        openai_key_present=bool(
            OPENAI_API_KEY
        ),

        connected=ctrader_state["connected"],

        account_authorized=ctrader_state[
            "account_authorized"
        ],

        market_ready=ctrader_state[
            "market_ready"
        ],

        last_market_refresh=ctrader_state[
            "last_market_refresh"
        ],

        observer=observer_state,

        error=ctrader_state["error"]

    )


@app.route("/health")
def health():

    return jsonify(

        status="ok",

        version="V11_AI_OBSERVER",

        demo_only=True,

        trading_enabled=False

    )


@app.route("/ai/status")
def ai_status():

    return jsonify(

        openai_key_present=bool(
            OPENAI_API_KEY
        ),

        model=AI_MODEL,

        **observer_state

    )


@app.route("/market")
def market():

    m15 = closed_candles(
        "M15"
    )

    h1 = closed_candles(
        "H1"
    )

    return jsonify(

        market_ready=ctrader_state[
            "market_ready"
        ],

        symbol=market_state["US100"][
            "symbol_name"
        ],

        m15_bars=len(
            m15
        ),

        h1_bars=len(
            h1
        ),

        M15_regression=regression_channel(
            m15
        ),

        H1_regression=regression_channel(
            h1
        ),

        MONTH_regression=monthly_regression(
            m15
        )

    )


@app.route("/telegram/test")
def telegram_test():

    ok = send_telegram_message(

        "US100 V11 AI OBSERVER\n\n"

        "Telegram działa.\n"

        "ENTRY / SL / TP: gotowe.\n"

        "BRAK HANDLU."

    )

    return jsonify(

        status=(
            "success"
            if ok
            else "error"
        ),

        telegram_sent=ok

    )


# ============================================================
# WEBHOOK
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    if not WEBHOOK_SECRET:

        return jsonify(

            status="error",

            message="WEBHOOK_SECRET not configured"

        ), 503

    received_secret = request.args.get(
        "secret"
    )

    if received_secret != WEBHOOK_SECRET:

        return jsonify(

            status="error",

            message="invalid secret"

        ), 403

    if request.is_json:

        data = request.get_json(
            silent=True
        )

        if isinstance(
            data,
            dict
        ):

            text = (

                data.get("message")

                or data.get("text")

                or json.dumps(
                    data,
                    ensure_ascii=False
                )

            )

        else:

            text = str(
                data or ""
            )

    else:

        text = request.get_data(
            as_text=True
        )

    if not str(text).strip():

        return jsonify(

            status="error",

            message="empty alert"

        ), 400

    threading.Thread(

        target=process_strategy_alert,

        args=(text,),

        daemon=True

    ).start()

    return jsonify(

        status="accepted",

        mode="V11_AI_OBSERVER_ONLY",

        trading=False

    ), 200


# ============================================================
# MARKET REFRESH
# ============================================================

@app.route("/market/refresh")
def market_refresh():

    if (
        ctrader_client is None
        or not ctrader_state["account_authorized"]
    ):

        return jsonify(

            status="error",

            message="cTrader not authorized"

        ), 503

    reactor.callFromThread(
        refresh_market
    )

    return jsonify(
        status="refresh_started"
    )


# ============================================================
# CTRADER LOGIN
# ============================================================

@app.route("/ctrader/login")
def ctrader_login():

    if not all((

        CTRADER_CLIENT_ID,

        CTRADER_CLIENT_SECRET,

        CTRADER_REDIRECT_URI,

    )):

        return jsonify(

            status="error",

            message="Missing cTrader configuration"

        ), 503

    url = (

        "https://id.ctrader.com/my/settings/"
        "openapi/grantingaccess/?"

        + urlencode({

            "client_id":
                CTRADER_CLIENT_ID,

            "redirect_uri":
                CTRADER_REDIRECT_URI,

            "scope":
                "accounts",

            "product":
                "web",

        })

    )

    return redirect(
        url
    )


# ============================================================
# CTRADER CALLBACK
# ============================================================

@app.route("/ctrader/callback")
def ctrader_callback():

    code = request.args.get(
        "code"
    )

    if not code:

        return jsonify(

            status="error",

            message="Brak kodu OAuth"

        ), 400

    try:

        response = requests.get(

            "https://openapi.ctrader.com/apps/token",

            params={

                "grant_type":
                    "authorization_code",

                "code":
                    code,

                "redirect_uri":
                    CTRADER_REDIRECT_URI,

                "client_id":
                    CTRADER_CLIENT_ID,

                "client_secret":
                    CTRADER_CLIENT_SECRET,

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
                "No accessToken"
            )

        save_state()

    except Exception as exc:

        set_error(
            exc
        )

        return jsonify(

            status="error",

            message="Token error"

        ), 500

    if not ensure_reactor():

        return jsonify(

            status="error",

            message="Reactor not running"

        ), 503

    reactor.callFromThread(
        start_ctrader_connection
    )

    return jsonify(

        status="success",

        route="DEMO",

        version="V11_AI_OBSERVER",

        trading_enabled=False,

        target_trader_login=TARGET_TRADER_LOGIN

    )


# ============================================================
# START
# ============================================================

threading.Thread(

    target=bootstrap,

    daemon=True

).start()


if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        )
