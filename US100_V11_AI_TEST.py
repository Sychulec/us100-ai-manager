# ============================================================
# US100 V11 AI OBSERVER - DEMO ONLY
#
# TradingView -> V11 -> cTrader market data -> OpenAI -> Telegram
#
# UWAGA:
# - TEN PROGRAM NIE OTWIERA TRANSAKCJI
# - NIE ZAMYKA TRANSAKCJI
# - NIE ZMIENIA SL/TP
# - AI JEST TYLKO OBSERWATOREM
# ============================================================

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


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

TARGET_TRADER_LOGIN = 5901967

BOT_NAME = "US100_V11_AI_OBSERVER"

REG_LENGTH = int(
    os.environ.get(
        "REG_LENGTH",
        "100",
    )
)

REG_DEV = float(
    os.environ.get(
        "REG_DEV",
        "2.0",
    )
)

AUTO_REFRESH_SECONDS = int(
    os.environ.get(
        "AUTO_REFRESH_SECONDS",
        "60",
    )
)

AI_MODEL = os.environ.get(
    "OPENAI_MODEL",
    "gpt-5",
)

OPENAI_API_KEY = os.environ.get(
    "OPENAI_API_KEY",
    "",
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
    "TELEGRAM_TOKEN",
    "",
)

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID",
    "",
)

WEBHOOK_SECRET = os.environ.get(
    "WEBHOOK_SECRET",
    "",
)


# ============================================================
# STATE DIRECTORY
# ============================================================

def choose_state_dir():

    configured = os.environ.get(
        "BOT_STATE_DIR"
    )

    candidates = (
        [configured]
        if configured
        else [
            "/var/data",
            "/tmp/us100-v11-ai",
        ]
    )

    for path in candidates:

        if not path:
            continue

        try:

            os.makedirs(
                path,
                exist_ok=True,
            )

            probe = os.path.join(
                path,
                ".v11_write_test",
            )

            with open(
                probe,
                "w",
                encoding="utf-8",
            ) as handle:

                handle.write("ok")

            os.remove(probe)

            return path

        except Exception:
            pass

    return tempfile.gettempdir()


BOT_STATE_DIR = choose_state_dir()

BOT_STATE_FILE = os.path.join(
    BOT_STATE_DIR,
    "us100_v11_ai_state.json",
)

AI_LOG_FILE = os.path.join(
    BOT_STATE_DIR,
    "us100_v11_ai_decisions.jsonl",
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
}


ctrader_client = None

reactor_started = False

refresh_started = False

state_lock = threading.Lock()


print(
    "[BOOT] US100 V11 AI OBSERVER | "
    f"DEMO login={TARGET_TRADER_LOGIN} | "
    "NO TRADING",
    flush=True,
)


# ============================================================
# BASIC HELPERS
# ============================================================

def set_error(error):

    ctrader_state["error"] = str(error)

    print(
        "[ERROR]",
        str(error),
        flush=True,
    )


def clear_error():

    ctrader_state["error"] = None


def safe_errback(failure):

    set_error(
        str(failure)
    )


def normalize_symbol(name):

    return re.sub(
        r"[^A-Z0-9]",
        "",
        str(name or "").upper(),
    )


def detect_instrument(name):

    value = normalize_symbol(name)

    if any(
        item in value
        for item in (
            "US100",
            "NAS100",
            "USTEC",
            "USTECH100",
            "NASDAQ100",
        )
    ):

        return "US100"

    return None


def strip_accents(value):

    value = unicodedata.normalize(
        "NFKD",
        str(value),
    )

    return "".join(
        ch
        for ch in value
        if not unicodedata.combining(ch)
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_message(text):

    if (
        not TELEGRAM_TOKEN
        or not TELEGRAM_CHAT_ID
    ):

        print(
            "[TELEGRAM DISABLED]",
            text,
            flush=True,
        )

        return False

    try:

        response = requests.post(

            f"https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/sendMessage",

            data={
                "chat_id":
                    TELEGRAM_CHAT_ID,

                "text":
                    text,
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


# ============================================================
# SAVE / LOAD
# ============================================================

def save_state():

    payload = {

        "access_token":
            ctrader_state.get(
                "access_token"
            ),

        "refresh_token":
            ctrader_state.get(
                "refresh_token"
            ),

        "observer_state":
            observer_state,
    }

    temp = (
        BOT_STATE_FILE
        + ".tmp"
    )

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

        print(
            "[STATE ERROR]",
            error,
            flush=True,
        )

        return False


def load_state():

    if not os.path.exists(
        BOT_STATE_FILE
    ):

        return False

    try:

        with state_lock:

            with open(
                BOT_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as handle:

                payload = json.load(
                    handle
                )

        ctrader_state[
            "access_token"
        ] = payload.get(
            "access_token"
        )

        ctrader_state[
            "refresh_token"
        ] = payload.get(
            "refresh_token"
        )

        saved_observer = payload.get(
            "observer_state",
            {},
        )

        if isinstance(
            saved_observer,
            dict,
        ):

            observer_state.update(
                saved_observer
            )

        return True

    except Exception as error:

        print(
            "[STATE LOAD ERROR]",
            error,
            flush=True,
        )

        return False


# ============================================================
# TRADINGVIEW PARSER
# ============================================================

def extract_number(
    label,
    text,
):

    match = re.search(

        rf"{label}\s*:\s*"
        rf"(-?[0-9]+(?:[.,][0-9]+)?)",

        text,

        re.I,
    )

    if not match:

        return None

    return float(
        match.group(1).replace(
            ",",
            ".",
        )
    )


def parse_strategy_alert(text):

    raw = str(
        text or ""
    ).strip()

    normalized = re.sub(

        r"\s+",

        " ",

        strip_accents(
            raw
        ).upper(),

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
            "ENTRY"
            if side
            else "UNKNOWN",

        "side":
            side,

        "symbol":
            detect_instrument(
                symbol_raw
            ),

        "timeframe":
            timeframe,

        "strategy_entry":
            extract_number(
                "CENA",
                normalized,
            ),

        "strategy_sl":
            extract_number(
                "SL",
                normalized,
            ),

        "strategy_tp":
            extract_number(
                "TP",
                normalized,
            ),

        "raw":
            raw,
    }


# ============================================================
# CANDLES
# ============================================================

def trendbar_to_dict(
    bar,
    digits,
):

    low = int(
        bar.low
    )

    return {

        "timestamp":
            int(
                bar.utcTimestampInMinutes
            ) * 60,

        "open":
            round(
                (
                    low
                    + int(
                        bar.deltaOpen
                    )
                )
                / 100000.0,
                digits,
            ),

        "high":
            round(
                (
                    low
                    + int(
                        bar.deltaHigh
                    )
                )
                / 100000.0,
                digits,
            ),

        "low":
            round(
                low / 100000.0,
                digits,
            ),

        "close":
            round(
                (
                    low
                    + int(
                        bar.deltaClose
                    )
                )
                / 100000.0,
                digits,
            ),

        "volume":
            int(
                bar.volume
            ),
    }


def closed_candles(
    timeframe,
):

    candles = list(
        market_state[
            "US100"
        ][
            "candles"
        ].get(
            timeframe,
            [],
        )
    )

    if not candles:

        return []

    now = int(
        time.time()
    )

    seconds = (
        900
        if timeframe == "M15"
        else 3600
    )

    return [

        candle

        for candle in candles

        if (
            int(
                candle["timestamp"]
            )
            + seconds
            <= now
        )
    ]


# ============================================================
# REGRESSION CHANNEL
# ============================================================

def regression_channel(
    candles,
    length=None,
):

    length = (
        length
        or REG_LENGTH
    )

    if len(candles) < length:

        return None

    window = candles[
        -length:
    ]

    y = [
        float(
            candle["close"]
        )
        for candle in window
    ]

    n = len(y)

    mean_x = (
        n - 1
    ) / 2.0

    mean_y = (
        sum(y)
        / n
    )

    var_x = sum(
        (
            i - mean_x
        ) ** 2
        for i in range(n)
    )

    if var_x <= 0:

        return None

    cov = sum(

        (
            i - mean_x
        )
        * (
            y[i] - mean_y
        )

        for i in range(n)
    )

    slope = (
        cov
        / var_x
    )

    intercept = (
        mean_y
        - slope
        * mean_x
    )

    fitted = [

        intercept
        + slope * i

        for i in range(n)
    ]

    residuals = [

        y[i]
        - fitted[i]

        for i in range(n)
    ]

    stdev = math.sqrt(

        sum(
            value * value
            for value in residuals
        )
        / n
    )

    basis = fitted[-1]

    upper = (
        basis
        + REG_DEV
        * stdev
    )

    lower = (
        basis
        - REG_DEV
        * stdev
    )

    width = (
        upper
        - lower
    )

    return {

        "basis":
            basis,

        "upper":
            upper,

        "lower":
            lower,

        "slope":
            slope,

        "stdev":
            stdev,

        "width":
            width,

        "bar_ts":
            int(
                window[-1][
                    "timestamp"
                ]
            ),
    }


# ============================================================
# MONTHLY REGRESSION
# ============================================================

def monthly_regression(
    candles,
):

    if not candles:

        return None

    now = datetime.now(
        timezone.utc
    )

    monthly = []

    for candle in candles:

        candle_dt = (
            datetime.fromtimestamp(
                int(
                    candle[
                        "timestamp"
                    ]
                ),
                timezone.utc,
            )
        )

        if (
            candle_dt.year
            == now.year

            and candle_dt.month
            == now.month
        ):

            monthly.append(
                candle
            )

    if len(monthly) < 10:

        return None

    return regression_channel(
        monthly,
        length=len(monthly),
    )


# ============================================================
# PRICE LOCATION
# ============================================================

def channel_position(
    price,
    channel,
):

    if (
        channel is None
        or price is None
    ):

        return None

    upper = float(
        channel["upper"]
    )

    lower = float(
        channel["lower"]
    )

    basis = float(
        channel["basis"]
    )

    width = (
        upper - lower
    )

    if width <= 0:

        return None

    normalized = (
        (
            float(price)
            - lower
        )
        / width
    )

    if price > upper:

        zone = "ABOVE_UPPER"

    elif price > basis:

        zone = "UPPER_HALF"

    elif price < lower:

        zone = "BELOW_LOWER"

    elif price < basis:

        zone = "LOWER_HALF"

    else:

        zone = "BASIS"

    return {

        "zone":
            zone,

        "position_0_1":
            round(
                normalized,
                4,
            ),

        "distance_to_basis":
            round(
                float(price)
                - basis,
                4,
            ),
    }


# ============================================================
# PRICE STRUCTURE
# ============================================================

def structure_summary(
    candles,
):

    if len(candles) < 12:

        return {
            "structure":
                "UNKNOWN",
        }

    sample = candles[
        -12:
    ]

    first = sample[:6]

    second = sample[6:]

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

def band_walk_summary(
    candles,
):

    if len(candles) < REG_LENGTH + 8:

        return {
            "status":
                "UNKNOWN",
        }

    recent = candles[
        -8:
    ]

    upper_half = 0
    lower_half = 0

    for candle in recent:

        history = [

            item

            for item in candles

            if (
                item["timestamp"]
                <= candle["timestamp"]
            )
        ]

        channel = regression_channel(
            history
        )

        if not channel:

            continue

        close = float(
            candle["close"]
        )

        if close > channel["basis"]:

            upper_half += 1

        elif close < channel["basis"]:

            lower_half += 1

    if upper_half >= 6:

        status = (
            "WALKING_UPPER_HALF"
        )

    elif lower_half >= 6:

        status = (
            "WALKING_LOWER_HALF"
        )

    else:

        status = "BALANCED"

    return {

        "status":
            status,

        "upper_half_bars":
            upper_half,

        "lower_half_bars":
            lower_half,
    }


# ============================================================
# VOLUME CONTEXT
# ============================================================

def volume_summary(
    candles,
):

    if len(candles) < 21:

        return {
            "status":
                "UNKNOWN",
        }

    previous = candles[
        -21:-1
    ]

    last = candles[-1]

    average = (
        sum(
            c["volume"]
            for c in previous
        )
        / len(previous)
    )

    ratio = (
        last["volume"]
        / average
        if average > 0
        else 0
    )

    if ratio >= 1.5:

        status = "VERY_HIGH"

    elif ratio >= 1.15:

        status = "HIGH"

    elif ratio <= 0.70:

        status = "LOW"

    else:

        status = "NORMAL"

    return {

        "status":
            status,

        "last_volume":
            last["volume"],

        "average_20":
            round(
                average,
                2,
            ),

        "ratio":
            round(
                ratio,
                3,
            ),
    }


# ============================================================
# MARKET SNAPSHOT
# ============================================================

def build_market_snapshot(
    signal,
):

    m15 = closed_candles(
        "M15"
    )

    h1 = closed_candles(
        "H1"
    )

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

    month_reg = monthly_regression(
        m15
    )

    if (
        not m15_reg
        or not h1_reg
    ):

        return None

    last_m15 = m15[-1]

    last_h1 = h1[-1]

    current_price = float(
        last_m15["close"]
    )

    # Tylko ograniczona liczba świec wysyłana do AI.
    # AI nie potrzebuje wszystkich 250.
    recent_m15 = m15[
        -20:
    ]

    recent_h1 = h1[
        -12:
    ]

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
                signal[
                    "strategy_entry"
                ],

            "strategy_sl":
                signal[
                    "strategy_sl"
                ],

            "strategy_tp":
                signal[
                    "strategy_tp"
                ],

            "timeframe":
                signal[
                    "timeframe"
                ],
        },

        "market_price":
            current_price,

        "M15": {

            "last_closed":
                last_m15,

            "regression": {
                key:
                    round(value, 5)
                    if isinstance(
                        value,
                        float,
                    )
                    else value

                for key, value
                in m15_reg.items()
            },

            "location":
                channel_position(
                    current_price,
                    m15_reg,
                ),

            "structure":
                structure_summary(
                    m15
                ),

            "band_walk":
                band_walk_summary(
                    m15
                ),

            "volume":
                volume_summary(
                    m15
                ),

            "recent_candles":
                recent_m15,
        },

        "H1": {

            "last_closed":
                last_h1,

            "regression": {
                key:
                    round(value, 5)
                    if isinstance(
                        value,
                        float,
                    )
                    else value

                for key, value
                in h1_reg.items()
            },

            "location":
                channel_position(
                    float(
                        last_h1[
                            "close"
                        ]
                    ),
                    h1_reg,
                ),

            "structure":
                structure_summary(
                    h1
                ),

            "band_walk":
                band_walk_summary(
                    h1
                ),

            "volume":
                volume_summary(
                    h1
                ),

            "recent_candles":
                recent_h1,
        },

        "MONTH": (

            {

                "regression": {
                    key:
                        round(
                            value,
                            5,
                        )
                        if isinstance(
                            value,
                            float,
                        )
                        else value

                    for key, value
                    in month_reg.items()
                },

                "location":
                    channel_position(
                        current_price,
                        month_reg,
                    ),
            }

            if month_reg
            else None
        ),
    }


# ============================================================
# AI PROMPT
# ============================================================

AI_INSTRUCTIONS = """
Jestes filtrem wejsc dla strategii regresji US100.

To jest eksperyment obserwacyjny.
NIE wydajesz zlecen brokerskich.
NIE zarzadzasz pozycja.
Oceniasz tylko kandydacki sygnal TradingView.

Strategia TradingView:
- pracuje na M15,
- wykorzystuje 100-swiecowy tunel regresji liniowej,
- LONG pojawia sie po powrocie ceny nad dolna bande,
- SHORT pojawia sie po powrocie ceny pod gorna bande.

Najwazniejszy problem:
strategia dobrze radzi sobie w szerokiej konsolidacji,
ale moze generowac zle wejscia przeciwko silnemu trendowi,
gdy cena prowadzi sie wzdluz zewnetrznej bandy.

Oceniaj przede wszystkim:

1. kierunek i nachylenie regresji M15,
2. czy cena utrzymuje sie w gornej lub dolnej polowie tunelu,
3. czy wystepuje band-walking,
4. strukture ceny HH/HL albo LH/LL,
5. kontekst H1,
6. kontekst miesiecznego tunelu,
7. swiece i wolumen,
8. czy sygnal jest zgodny czy przeciwny do dominujacego ruchu.

Nie odrzucaj sygnalu tylko dlatego, ze jest przy zewnetrznej bandzie.
To normalne dla tej strategii.

Szukamy przede wszystkim sytuacji, gdy wejscie przy bandzie jest
niebezpiecznym kontrtrendowym wejsciem podczas silnego trendu.

Nie przewiduj przyszlosci z pewnoscia.
Ocena oznacza jakosc sygnalu, a nie gwarancje wyniku.

Klasyfikacja rynku musi byc jedna z:
TREND_WZROSTOWY
TREND_SPADKOWY
KONSOLIDACJA
PRZEJSCIE
NIEJASNY

Decyzja musi byc:
AKCEPTUJ
ODRZUC
OBSERWUJ

Score:
0-100.

AKCEPTUJ:
sygnal ma sens strukturalny.

ODRZUC:
sygnal jest wyraznie niebezpieczny lub przeciwny
do dominujacego ruchu.

OBSERWUJ:
brakuje wystarczajacego potwierdzenia.

Powod ma byc krotki i konkretny.
"""


# ============================================================
# OPENAI
# ============================================================

def call_openai(
    snapshot,
):

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
                "Ocen ponizszy sygnal US100. "
                "Dane sa snapshotem dostepnym "
                "w chwili analizy.\n\n"
                + json.dumps(
                    snapshot,
                    ensure_ascii=False,
                    separators=(
                        ",",
                        ":",
                    ),
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

        timeout=45,
    )

    if not response.ok:

        raise RuntimeError(
            "OpenAI HTTP "
            + str(
                response.status_code
            )
            + ": "
            + response.text[:500]
        )

    data = response.json()

    output_text = ""

    for item in data.get(
        "output",
        [],
    ):

        if item.get(
            "type"
        ) != "message":

            continue

        for content in item.get(
            "content",
            [],
        ):

            if content.get(
                "type"
            ) == "output_text":

                output_text += str(
                    content.get(
                        "text",
                        "",
                    )
                )

    if not output_text:

        raise RuntimeError(
            "OpenAI zwrocil pusty output"
        )

    result = json.loads(
        output_text
    )

    return result


# ============================================================
# LOG DECISIONS
# ============================================================

def save_ai_decision(
    signal,
    snapshot,
    result,
):

    record = {

        "timestamp":
            int(
                time.time()
            ),

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
                encoding="utf-8",
            ) as handle:

                handle.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        return True

    except Exception as error:

        print(
            "[AI LOG ERROR]",
            error,
            flush=True,
        )

        return False


# ============================================================
# AI ANALYSIS
# ============================================================

def analyse_signal_with_ai(
    signal,
):

    observer_state[
        "signals_received"
    ] += 1

    observer_state[
        "last_signal"
    ] = signal

    save_state()

    snapshot = build_market_snapshot(
        signal
    )

    if snapshot is None:

        print(
            "[AI] MARKET NOT READY",
            flush=True,
        )

        send_telegram_message(
            "US100 V11 AI OBSERVER\n\n"
            f"Sygnal: {signal['side']}\n"
            "AI: brak analizy\n"
            "Powod: dane M15/H1 nie sa jeszcze gotowe."
        )

        return

    try:

        observer_state[
            "ai_calls"
        ] += 1

        result = call_openai(
            snapshot
        )

        observer_state[
            "last_ai_decision"
        ] = result

        save_state()

        save_ai_decision(
            signal,
            snapshot,
            result,
        )

        decision = result.get(
            "decision",
            "OBSERWUJ",
        )

        score = result.get(
            "score",
            0,
        )

        regime = result.get(
            "market_regime",
            "NIEJASNY",
        )

        alignment = result.get(
            "signal_alignment",
            "NEUTRALNY",
        )

        reason = result.get(
            "reason",
            "",
        )

        print(
            "[AI DECISION]",
            decision,
            score,
            regime,
            reason,
            flush=True,
        )

        send_telegram_message(

            "US100 V11 AI OBSERVER\n\n"

            f"TradingView: {signal['side']}\n"

            f"Cena sygnalu: "
            f"{signal['strategy_entry']:.2f}\n\n"

            f"AI: {decision}\n"

            f"Ocena: {score}/100\n"

            f"Rynek: {regime}\n"

            f"Uklad: {alignment}\n\n"

            f"Powod:\n{reason}\n\n"

            "TYLKO OBSERWACJA - BRAK TRANSAKCJI"
        )

    except Exception as error:

        observer_state[
            "ai_errors"
        ] += 1

        observer_state[
            "last_ai_decision"
        ] = {

            "error":
                str(error),
        }

        save_state()

        print(
            "[AI ERROR]",
            error,
            flush=True,
        )

        send_telegram_message(

            "US100 V11 AI OBSERVER\n\n"

            f"Sygnal: {signal['side']}\n"

            "AI ERROR\n"

            f"{str(error)[:300]}\n\n"

            "BRAK TRANSAKCJI"
        )


# ============================================================
# PROCESS TRADINGVIEW ALERT
# ============================================================

def process_strategy_alert(
    text,
):

    print(
        "[TV] RAW:",
        str(text).replace(
            "\n",
            " | ",
        )[:1000],
        flush=True,
    )

    signal = parse_strategy_alert(
        text
    )

    print(
        "[TV] PARSED",
        signal,
        flush=True,
    )

    if (
        signal["event"]
        != "ENTRY"
    ):

        print(
            "[TV] REJECTED: event",
            flush=True,
        )

        return

    if signal[
        "side"
    ] not in (
        "LONG",
        "SHORT",
    ):

        print(
            "[TV] REJECTED: side",
            flush=True,
        )

        return

    if signal[
        "symbol"
    ] != "US100":

        print(
            "[TV] REJECTED: symbol",
            flush=True,
        )

        return

    if signal[
        "timeframe"
    ] not in (
        "15",
        "15m",
        "15min",
        "m15",
    ):

        print(
            "[TV] REJECTED: timeframe",
            flush=True,
        )

        return

    if None in (

        signal[
            "strategy_entry"
        ],

        signal[
            "strategy_sl"
        ],

        signal[
            "strategy_tp"
        ],

    ):

        print(
            "[TV] REJECTED: missing values",
            flush=True,
        )

        return

    print(
        "[TV] ACCEPTED -> AI OBSERVER",
        flush=True,
    )

    analyse_signal_with_ai(
        signal
    )


# ============================================================
# CTRADER TREND BARS
# ============================================================

def request_trendbars(
    client,
    timeframe,
):

    symbol_id = market_state[
        "US100"
    ][
        "symbol_id"
    ]

    if (
        not ctrader_state[
            "account_authorized"
        ]
        or not symbol_id
    ):

        return

    now_ms = int(
        time.time()
        * 1000
    )

    req = (
        ProtoOAGetTrendbarsReq()
    )

    req.ctidTraderAccountId = int(
        ctrader_state[
            "account_id"
        ]
    )

    req.symbolId = int(
        symbol_id
    )

    if timeframe == "M15":

        req.period = (
            ProtoOATrendbarPeriod.M15
        )

        days = 35

    else:

        req.period = (
            ProtoOATrendbarPeriod.H1
        )

        days = 30

    req.fromTimestamp = (
        now_ms
        - days
        * 86400000
    )

    req.toTimestamp = (
        now_ms
    )

    req.count = 1500

    client.send(
        req
    ).addErrback(
        safe_errback
    )


# ============================================================
# ACCOUNT DATA
# ============================================================

def request_account_data(
    client,
):

    if not ctrader_state[
        "account_authorized"
    ]:

        return

    if not market_state[
        "US100"
    ][
        "found"
    ]:

        req = (
            ProtoOASymbolsListReq()
        )

        req.ctidTraderAccountId = int(
            ctrader_state[
                "account_id"
            ]
        )

        req.includeArchivedSymbols = (
            False
        )

        client.send(
            req
        ).addErrback(
            safe_errback
        )


# ============================================================
# REFRESH
# ============================================================

def refresh_market():

    if (
        ctrader_client is None
        or not ctrader_state[
            "account_authorized"
        ]
    ):

        return

    if market_state[
        "US100"
    ][
        "digits"
    ] is not None:

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
# CTRADER AUTH
# ============================================================

def send_account_list(
    client,
):

    token = ctrader_state.get(
        "access_token"
    )

    if not token:

        set_error(
            "Brak access_token"
        )

        return

    req = (
        ProtoOAGetAccountListByAccessTokenReq()
    )

    req.accessToken = token

    client.send(
        req
    ).addErrback(
        safe_errback
    )


def authorize_account(
    client,
):

    req = (
        ProtoOAAccountAuthReq()
    )

    req.ctidTraderAccountId = int(
        ctrader_state[
            "account_id"
        ]
    )

    req.accessToken = (
        ctrader_state[
            "access_token"
        ]
    )

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
        and ctrader_state[
            "connected"
        ]
    ):

        send_account_list(
            ctrader_client
        )

        return

    # HARD DEMO ENDPOINT.
    # V11 NIE laczy sie z LIVE.
    ctrader_client = Client(

        EndPoints.PROTOBUF_DEMO_HOST,

        EndPoints.PROTOBUF_PORT,

        TcpProtocol,
    )

    def connected(
        client,
    ):

        ctrader_state[
            "connected"
        ] = True

        clear_error()

        print(
            "[CTRADER] CONNECTED DEMO",
            flush=True,
        )

        req = (
            ProtoOAApplicationAuthReq()
        )

        req.clientId = (
            CTRADER_CLIENT_ID
        )

        req.clientSecret = (
            CTRADER_CLIENT_SECRET
        )

        client.send(
            req
        ).addErrback(
            safe_errback
        )


    def disconnected(
        client,
        reason,
    ):

        ctrader_state[
            "connected"
        ] = False

        ctrader_state[
            "account_authorized"
        ] = False

        print(
            "[CTRADER] DISCONNECTED",
            reason,
            flush=True,
        )


    def on_message(
        client,
        message,
    ):

        payload_type = (
            message.payloadType
        )

        # APPLICATION AUTH
        if (
            payload_type
            == ProtoOAApplicationAuthRes().payloadType
        ):

            ctrader_state[
                "application_authorized"
            ] = True

            send_account_list(
                client
            )


        # ACCOUNT LIST
        elif (
            payload_type
            == ProtoOAGetAccountListByAccessTokenRes().payloadType
        ):

            response = (
                Protobuf.extract(
                    message
                )
            )

            matches = [

                account

                for account
                in response.ctidTraderAccount

                if int(
                    getattr(
                        account,
                        "traderLogin",
                        0,
                    )
                    or 0
                )
                == TARGET_TRADER_LOGIN
            ]

            if not matches:

                set_error(
                    "Nie znaleziono DEMO "
                    f"traderLogin={TARGET_TRADER_LOGIN}"
                )

                return

            account = matches[0]

            # HARD LIVE BLOCK
            if bool(
                getattr(
                    account,
                    "isLive",
                    False,
                )
            ):

                ctrader_state[
                    "account_id"
                ] = None

                ctrader_state[
                    "account_authorized"
                ] = False

                set_error(
                    "LIVE BLOCKED - "
                    "V11 AI Observer jest tylko DEMO"
                )

                return

            ctrader_state[
                "account_id"
            ] = int(
                account.ctidTraderAccountId
            )

            print(
                "[CTRADER] DEMO TARGET FOUND "
                f"login={TARGET_TRADER_LOGIN} "
                f"account_id="
                f"{ctrader_state['account_id']}",
                flush=True,
            )

            authorize_account(
                client
            )


        # ACCOUNT AUTH
        elif (
            payload_type
            == ProtoOAAccountAuthRes().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = True

            clear_error()

            print(
                "[CTRADER] ACCOUNT AUTHORIZED DEMO",
                flush=True,
            )

            request_account_data(
                client
            )

            start_refresh_loop()


        # ACCOUNT DISCONNECT
        elif (
            payload_type
            == ProtoOAAccountDisconnectEvent().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = False

            reactor.callLater(
                1.0,
                authorize_account,
                client,
            )


        # TOKEN INVALID
        elif (
            payload_type
            == ProtoOAAccountsTokenInvalidatedEvent().payloadType
        ):

            ctrader_state[
                "account_authorized"
            ] = False

            set_error(
                "Token cTrader wygasl - "
                "zaloguj ponownie przez /ctrader/login"
            )


        # SYMBOL LIST
        elif (
            payload_type
            == ProtoOASymbolsListRes().payloadType
        ):

            response = (
                Protobuf.extract(
                    message
                )
            )

            candidates = [

                symbol

                for symbol
                in response.symbol

                if detect_instrument(
                    symbol.symbolName
                )
                == "US100"
            ]

            if not candidates:

                set_error(
                    "Nie znaleziono "
                    "US100 / US TECH 100"
                )

                return

            exact = [

                symbol

                for symbol
                in candidates

                if normalize_symbol(
                    symbol.symbolName
                )
                == "US100"
            ]

            symbol = (
                exact[0]
                if exact
                else candidates[0]
            )

            data = market_state[
                "US100"
            ]

            data[
                "found"
            ] = True

            data[
                "symbol_id"
            ] = int(
                symbol.symbolId
            )

            data[
                "symbol_name"
            ] = symbol.symbolName

            req = (
                ProtoOASymbolByIdReq()
            )

            req.ctidTraderAccountId = int(
                ctrader_state[
                    "account_id"
                ]
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


        # SYMBOL DETAILS
        elif (
            payload_type
            == ProtoOASymbolByIdRes().payloadType
        ):

            response = (
                Protobuf.extract(
                    message
                )
            )

            for symbol in response.symbol:

                if int(
                    symbol.symbolId
                ) != int(
                    market_state[
                        "US100"
                    ][
                        "symbol_id"
                    ]
                ):

                    continue

                data = market_state[
                    "US100"
                ]

                data[
                    "digits"
                ] = int(
                    symbol.digits
                )

                print(
                    "[CTRADER] SYMBOL READY "
                    f"{data['symbol_name']}",
                    flush=True,
                )

                request_trendbars(
                    client,
                    "M15",
                )

                request_trendbars(
                    client,
                    "H1",
                )


        # TREND BARS
        elif (
            payload_type
            == ProtoOAGetTrendbarsRes().payloadType
        ):

            response = (
                Protobuf.extract(
                    message
                )
            )

            symbol_id = market_state[
                "US100"
            ][
                "symbol_id"
            ]

            if not symbol_id:

                return

            if int(
                response.symbolId
            ) != int(
                symbol_id
            ):

                return

            if (
                response.period
                == ProtoOATrendbarPeriod.M15
            ):

                timeframe = "M15"

            elif (
                response.period
                == ProtoOATrendbarPeriod.H1
            ):

                timeframe = "H1"

            else:

                return

            digits = (
                market_state[
                    "US100"
                ][
                    "digits"
                ]
                or 2
            )

            candles = [

                trendbar_to_dict(
                    bar,
                    digits,
                )

                for bar
                in response.trendbar
            ]

            candles.sort(
                key=lambda item:
                    item["timestamp"]
            )

            market_state[
                "US100"
            ][
                "candles"
            ][
                timeframe
            ] = candles

            ctrader_state[
                "market_ready"
            ] = (

                len(
                    market_state[
                        "US100"
                    ][
                        "candles"
                    ][
                        "M15"
                    ]
                )
                >= REG_LENGTH

                and

                len(
                    market_state[
                        "US100"
                    ][
                        "candles"
                    ][
                        "H1"
                    ]
                )
                >= REG_LENGTH
            )

            ctrader_state[
                "last_market_refresh"
            ] = int(
                time.time()
            )


        # GENERIC ERROR
        elif (
            payload_type
            == ProtoOAErrorRes().payloadType
        ):

            response = (
                Protobuf.extract(
                    message
                )
            )

            code = str(
                response.errorCode
            )

            description = str(
                response.description
            )

            if (
                "ALREADY_LOGGED_IN"
                in code

                or
                "ALREADY_LOGGED_IN"
                in description
            ):

                if ctrader_state.get(
                    "account_id"
                ):

                    ctrader_state[
                        "account_authorized"
                    ] = True

                    request_account_data(
                        client
                    )

                else:

                    ctrader_state[
                        "application_authorized"
                    ] = True

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

    if not ctrader_state.get(
        "access_token"
    ):

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

        if getattr(
            reactor,
            "running",
            False,
        ):

            break

        time.sleep(
            0.1
        )

    if getattr(
        reactor,
        "running",
        False,
    ):

        reactor.callFromThread(
            start_ctrader_connection
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({

        "bot":
            BOT_NAME,

        "status":
            "ONLINE",

        "mode":
            "AI_OBSERVER_ONLY",

        "trading_enabled":
            False,

        "demo_only":
            True,

        "target_trader_login":
            TARGET_TRADER_LOGIN,

        "ai_model":
            AI_MODEL,

        "openai_key_present":
            bool(
                OPENAI_API_KEY
            ),

        "connected":
            ctrader_state[
                "connected"
            ],

        "account_authorized":
            ctrader_state[
                "account_authorized"
            ],

        "market_ready":
            ctrader_state[
                "market_ready"
            ],

        "last_market_refresh":
            ctrader_state[
                "last_market_refresh"
            ],

        "observer":
            observer_state,

        "error":
            ctrader_state[
                "error"
            ],
    })


@app.route("/health")
def health():

    return jsonify({

        "status":
            "ok",

        "version":
            "V11_AI_OBSERVER",

        "demo_only":
            True,

        "trading_enabled":
            False,
    })


@app.route("/ai/status")
def ai_status():

    return jsonify({

        "openai_key_present":
            bool(
                OPENAI_API_KEY
            ),

        "model":
            AI_MODEL,

        "signals_received":
            observer_state[
                "signals_received"
            ],

        "ai_calls":
            observer_state[
                "ai_calls"
            ],

        "ai_errors":
            observer_state[
                "ai_errors"
            ],

        "last_ai_decision":
            observer_state[
                "last_ai_decision"
            ],
    })


@app.route("/market")
def market():

    m15 = closed_candles(
        "M15"
    )

    h1 = closed_candles(
        "H1"
    )

    m15_reg = (
        regression_channel(
            m15
        )
        if len(m15) >= REG_LENGTH
        else None
    )

    h1_reg = (
        regression_channel(
            h1
        )
        if len(h1) >= REG_LENGTH
        else None
    )

    month_reg = (
        monthly_regression(
            m15
        )
        if m15
        else None
    )

    return jsonify({

        "market_ready":
            ctrader_state[
                "market_ready"
            ],

        "symbol":
            market_state[
                "US100"
            ][
                "symbol_name"
            ],

        "m15_bars":
            len(m15),

        "h1_bars":
            len(h1),

        "M15_regression":
            m15_reg,

        "H1_regression":
            h1_reg,

        "MONTH_regression":
            month_reg,
    })


@app.route(
    "/telegram/test"
)
def telegram_test():

    ok = send_telegram_message(

        "US100 V11 AI OBSERVER\n\n"

        "Telegram dziala.\n"

        "AI Observer TEST.\n"

        "BRAK HANDLU."
    )

    return jsonify({

        "status":
            "success"
            if ok
            else "error",

        "telegram_sent":
            ok,
    })


@app.route(
    "/webhook",
    methods=[
        "POST"
    ],
)
def webhook():

    if (
        WEBHOOK_SECRET

        and request.args.get(
            "secret"
        )
        != WEBHOOK_SECRET
    ):

        return jsonify({

            "status":
                "error",

            "message":
                "invalid secret",

        }), 403

    if request.is_json:

        data = request.get_json(
            silent=True
        )

        if isinstance(
            data,
            dict,
        ):

            text = (

                data.get(
                    "message"
                )

                or data.get(
                    "text"
                )

                or json.dumps(
                    data
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

    if not text.strip():

        return jsonify({

            "status":
                "error",

            "message":
                "empty alert",

        }), 400

    print(
        "[WEBHOOK] RECEIVED",
        flush=True,
    )

    threading.Thread(

        target=
            process_strategy_alert,

        args=(
            text,
        ),

        daemon=True,

    ).start()

    return jsonify({

        "status":
            "accepted",

        "mode":
            "V11_AI_OBSERVER_ONLY",

        "trading":
            False,

    }), 200


@app.route(
    "/market/refresh"
)
def market_refresh():

    if (
        ctrader_client is None

        or not ctrader_state[
            "account_authorized"
        ]
    ):

        return jsonify({

            "status":
                "error",

            "message":
                "cTrader not authorized",

        }), 503

    reactor.callFromThread(
        refresh_market
    )

    return jsonify({

        "status":
            "refresh_started",
    })


# ============================================================
# LOGIN
# ============================================================

@app.route(
    "/ctrader/login"
)
def ctrader_login():

    return redirect(

        "https://id.ctrader.com/"
        "my/settings/openapi/"
        "grantingaccess/?"

        + urlencode({

            "client_id":
                CTRADER_CLIENT_ID,

            "redirect_uri":
                CTRADER_REDIRECT_URI,

            # Read-only observer.
            "scope":
                "accounts",

            "product":
                "web",
        })
    )


@app.route(
    "/ctrader/callback"
)
def ctrader_callback():

    code = request.args.get(
        "code"
    )

    if not code:

        return jsonify({

            "status":
                "error",

            "message":
                "Brak kodu OAuth",

        }), 400

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

            timeout=15,
        )

        response.raise_for_status()

        data = response.json()

        ctrader_state[
            "access_token"
        ] = data.get(
            "accessToken"
        )

        ctrader_state[
            "refresh_token"
        ] = data.get(
            "refreshToken"
        )

        save_state()

    except Exception as error:

        set_error(
            error
        )

        return jsonify({

            "status":
                "error",

            "message":
                "Token error",

        }), 500

    if not reactor_started:

        threading.Thread(

            target=
                start_reactor,

            daemon=True,

        ).start()

        time.sleep(
            0.5
        )

    reactor.callFromThread(
        start_ctrader_connection
    )

    return jsonify({

        "status":
            "success",

        "route":
            "DEMO",

        "version":
            "V11_AI_OBSERVER",

        "trading_enabled":
            False,

        "target_trader_login":
            TARGET_TRADER_LOGIN,
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
