"""US100 V11 AI OBSERVER. DEMO, READ ONLY. TradingView -> Telegram -> AI."""
import os, re, time, math, json, tempfile, threading, unicodedata
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests
from flask import Flask, jsonify, redirect, request
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
    ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
    ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
    ProtoOAAccountDisconnectEvent, ProtoOAAccountsTokenInvalidatedEvent,
    ProtoOASymbolsListReq, ProtoOASymbolsListRes,
    ProtoOASymbolByIdReq, ProtoOASymbolByIdRes,
    ProtoOAGetTrendbarsReq, ProtoOAGetTrendbarsRes, ProtoOAErrorRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrendbarPeriod
from twisted.internet import reactor

app = Flask(__name__)
TARGET_TRADER_LOGIN = 5901967
BOT_NAME = 'US100_V11_AI_OBSERVER'
REG_LENGTH = max(5, int(os.getenv('REG_LENGTH', '100')))
REG_DEV = float(os.getenv('REG_DEV', '2.0'))
AUTO_REFRESH_SECONDS = max(30, int(os.getenv('AUTO_REFRESH_SECONDS', '60')))
AI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-5')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
CTRADER_CLIENT_ID = os.getenv('CTRADER_CLIENT_ID', '')
CTRADER_CLIENT_SECRET = os.getenv('CTRADER_CLIENT_SECRET', '')
CTRADER_REDIRECT_URI = os.getenv('CTRADER_REDIRECT_URI', '')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '')

def choose_state_dir():
    for path in ([os.getenv('BOT_STATE_DIR')] if os.getenv('BOT_STATE_DIR') else ['/var/data', '/tmp/us100-v11-ai']):
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, '.write_test')
            with open(probe, 'w') as f: f.write('ok')
            os.remove(probe)
            return path
        except Exception: pass
    return tempfile.gettempdir()

BOT_STATE_DIR = choose_state_dir()
BOT_STATE_FILE = os.path.join(BOT_STATE_DIR, 'us100_v11_ai_state.json')
AI_LOG_FILE = os.path.join(BOT_STATE_DIR, 'us100_v11_ai_decisions.jsonl')
ctrader_state = dict(access_token=None, refresh_token=None, connected=False,
    application_authorized=False, account_authorized=False, account_id=None,
    market_ready=False, last_market_refresh=None, error=None)
market_state = {'US100': dict(found=False, symbol_id=None, symbol_name=None,
    digits=None, candles={'M15': [], 'H1': []})}
observer_state = dict(signals_received=0, ai_calls=0, ai_errors=0,
    last_signal=None, last_ai_decision=None)
ctrader_client = None
reactor_started = False
refresh_started = False
state_lock = threading.RLock()
market_lock = threading.RLock()

def set_error(error):
    ctrader_state['error'] = str(error)
    print('[ERROR]', error, flush=True)
def safe_errback(failure): set_error(str(failure))
def normalize_symbol(name): return re.sub(r'[^A-Z0-9]', '', str(name or '').upper())
def detect_instrument(name):
    value = normalize_symbol(name)
    return 'US100' if any(x in value for x in ('US100','NAS100','USTEC','USTECH100','NASDAQ100')) else None

def send_telegram_message(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print('[TELEGRAM DISABLED]', text, flush=True)
        return False
    try:
        r = requests.post(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage',
                          data={'chat_id': TELEGRAM_CHAT_ID, 'text': text}, timeout=15)
        r.raise_for_status()
        return bool(r.json().get('ok'))
    except Exception as exc:
        print('[TELEGRAM ERROR]', exc, flush=True)
        return False

def save_state():
    payload = {'access_token': ctrader_state['access_token'],
               'refresh_token': ctrader_state['refresh_token'], 'observer_state': observer_state}
    try:
        with state_lock:
            temp = BOT_STATE_FILE + '.tmp'
            with open(temp, 'w', encoding='utf-8') as f: json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(temp, BOT_STATE_FILE)
        return True
    except Exception as exc:
        print('[STATE ERROR]', exc, flush=True)
        return False

def load_state():
    try:
        with state_lock:
            with open(BOT_STATE_FILE, encoding='utf-8') as f: payload = json.load(f)
        ctrader_state['access_token'] = payload.get('access_token')
        ctrader_state['refresh_token'] = payload.get('refresh_token')
        if isinstance(payload.get('observer_state'), dict): observer_state.update(payload['observer_state'])
        return True
    except FileNotFoundError: return False
    except Exception as exc:
        print('[STATE LOAD ERROR]', exc, flush=True)
        return False

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))
def extract_number(label, text):
    match = re.search(rf'\b{label}\s*:\s*(-?[0-9]+(?:[.,][0-9]+)?)', text, re.I)
    return float(match.group(1).replace(',', '.')) if match else None

def parse_strategy_alert(text):
    raw = str(text or '').strip()
    normalized = re.sub(r'\s+', ' ', strip_accents(raw).upper()).strip()
    side = 'LONG' if 'WEJSCIE LONG' in normalized else ('SHORT' if 'WEJSCIE SHORT' in normalized else None)
    symbol = re.search(r'\bSYMBOL\s*:\s*([^|]+)', normalized)
    tf = re.search(r'\bTF\s*:\s*([^|]+)', normalized)
    return dict(event='ENTRY' if side else 'UNKNOWN', side=side,
        symbol=detect_instrument(symbol.group(1).strip() if symbol else ''),
        timeframe=tf.group(1).strip().lower() if tf else '?',
        strategy_entry=extract_number('CENA', normalized),
        strategy_sl=extract_number('SL', normalized),
        strategy_tp=extract_number('TP', normalized), raw=raw)

def send_tradingview_signal(signal):
    """Always attempt to forward the strategy's levels BEFORE any AI work."""
    message = (f'US100 V11 | SYGNAL TRADINGVIEW\n\n'
               f'Kierunek: {signal["side"]}\n'
               f'ENTRY: {signal["strategy_entry"]:.2f}\n'
               f'SL: {signal["strategy_sl"]:.2f}\n'
               f'TP: {signal["strategy_tp"]:.2f}\n\n'
               'AI: analiza w osobnej wiadomosci\n'
               'TYLKO OBSERWACJA - BRAK TRANSAKCJI')
    return send_telegram_message(message)

def trendbar_to_dict(bar, digits):
    low = int(bar.low)
    return dict(timestamp=int(bar.utcTimestampInMinutes)*60,
        open=round((low+int(bar.deltaOpen))/100000.0, digits),
        high=round((low+int(bar.deltaHigh))/100000.0, digits),
        low=round(low/100000.0, digits),
        close=round((low+int(bar.deltaClose))/100000.0, digits), volume=int(bar.volume))

def closed_candles(tf):
    with market_lock: candles = list(market_state['US100']['candles'].get(tf, []))
    seconds = 900 if tf == 'M15' else 3600
    now = int(time.time())
    return [c for c in candles if c['timestamp']+seconds <= now]

def regression_channel(candles, length=None):
    length = length or REG_LENGTH
    if len(candles) < length: return None
    window = candles[-length:]
    y = [float(c['close']) for c in window]
    n = len(y); mx = (n-1)/2; my = sum(y)/n
    vx = sum((i-mx)**2 for i in range(n))
    if vx <= 0: return None
    slope = sum((i-mx)*(y[i]-my) for i in range(n))/vx
    intercept = my-slope*mx
    residuals = [y[i]-(intercept+slope*i) for i in range(n)]
    stdev = math.sqrt(sum(r*r for r in residuals)/n)
    basis = intercept+slope*(n-1)
    return dict(basis=basis, upper=basis+REG_DEV*stdev,
        lower=basis-REG_DEV*stdev, slope=slope, stdev=stdev,
        width=2*REG_DEV*stdev, bar_ts=int(window[-1]['timestamp']))

def monthly_regression(candles):
    now = datetime.now(timezone.utc)
    monthly = [c for c in candles if (lambda d: d.year == now.year and d.month == now.month)(
        datetime.fromtimestamp(int(c['timestamp']), timezone.utc))]
    return regression_channel(monthly, len(monthly)) if len(monthly) >= 10 else None

def channel_position(price, channel):
    if channel is None or price is None: return None
    upper, lower, basis = channel['upper'], channel['lower'], channel['basis']
    if upper <= lower: return None
    zone = ('ABOVE_UPPER' if price > upper else 'UPPER_HALF' if price > basis else
            'BELOW_LOWER' if price < lower else 'LOWER_HALF' if price < basis else 'BASIS')
    return dict(zone=zone, position_0_1=round((price-lower)/(upper-lower),4),
                distance_to_basis=round(price-basis,4))

def structure_summary(candles):
    if len(candles) < 12: return {'structure': 'UNKNOWN'}
    a,b = candles[-12:-6], candles[-6:]
    ah,bh = max(c['high'] for c in a), max(c['high'] for c in b)
    al,bl = min(c['low'] for c in a), min(c['low'] for c in b)
    label = 'HH_HL' if bh>ah and bl>al else 'LH_LL' if bh<ah and bl<al else 'MIXED'
    return dict(structure=label, first_high=ah, second_high=bh, first_low=al, second_low=bl)

def band_walk_summary(candles):
    if len(candles) < REG_LENGTH+8: return {'status':'UNKNOWN'}
    upper = lower = 0
    for i in range(len(candles)-8, len(candles)):
        channel = regression_channel(candles[:i+1])
        if channel:
            upper += candles[i]['close'] > channel['basis']
            lower += candles[i]['close'] < channel['basis']
    status = 'WALKING_UPPER_HALF' if upper>=6 else 'WALKING_LOWER_HALF' if lower>=6 else 'BALANCED'
    return dict(status=status, upper_half_bars=upper, lower_half_bars=lower)

def volume_summary(candles):
    if len(candles)<21: return {'status':'UNKNOWN'}
    avg = sum(c['volume'] for c in candles[-21:-1])/20
    last = candles[-1]['volume']; ratio = last/avg if avg else 0
    status = 'VERY_HIGH' if ratio>=1.5 else 'HIGH' if ratio>=1.15 else 'LOW' if ratio<=.7 else 'NORMAL'
    return dict(status=status, last_volume=last, average_20=round(avg,2), ratio=round(ratio,3))

def build_market_snapshot(signal):
    m15, h1 = closed_candles('M15'), closed_candles('H1')
    if len(m15)<REG_LENGTH or len(h1)<REG_LENGTH: return None
    mreg,hreg = regression_channel(m15),regression_channel(h1)
    if not mreg or not hreg: return None
    month = monthly_regression(m15)
    price = float(m15[-1]['close'])
    def block(candles, reg, current_price, recent):
        return dict(last_closed=candles[-1], regression=reg,
            location=channel_position(current_price,reg), structure=structure_summary(candles),
            band_walk=band_walk_summary(candles), volume=volume_summary(candles),
            recent_candles=candles[-recent:])
    return dict(instrument='US100', observer_version='V11',
        signal_time_utc=datetime.now(timezone.utc).isoformat(),
        signal=dict(side=signal['side'], entry=signal['strategy_entry'],
                    strategy_sl=signal['strategy_sl'], strategy_tp=signal['strategy_tp'],
                    timeframe=signal['timeframe']), market_price=price,
        M15=block(m15,mreg,price,20), H1=block(h1,hreg,float(h1[-1]['close']),12),
        MONTH={'regression':month,'location':channel_position(price,month)} if month else None)

AI_INSTRUCTIONS = '''Jestes filtrem wejsc strategii regresji US100. TYLKO OBSERWACJA.
Nie wydajesz zlecen i nie zmieniasz ENTRY, SL ani TP.
Strategia M15 ma 100-swiecowy tunel regresji. LONG po powrocie nad dolna bande,
SHORT po powrocie pod gorna bande. Szczegolnie wykrywaj kontrtrendowe wejscia
w silnym trendzie i band-walking. Uwzglednij nachylenie i polowe tunelu M15,
strukture HH/HL lub LH/LL, H1, tunel miesieczny, swiece i wolumen.
Nie odrzucaj tylko za wejscie przy bandzie. Nie twierdz, ze znasz przyszlosc.
market_regime: TREND_WZROSTOWY, TREND_SPADKOWY, KONSOLIDACJA, PRZEJSCIE, NIEJASNY.
decision: AKCEPTUJ, ODRZUC, OBSERWUJ. score: 0-100. Krotki konkretny powod.'''

def call_openai(snapshot):
    if not OPENAI_API_KEY: raise RuntimeError('Brak OPENAI_API_KEY')
    schema = {'type':'object','properties':{
        'decision':{'type':'string','enum':['AKCEPTUJ','ODRZUC','OBSERWUJ']},
        'score':{'type':'integer','minimum':0,'maximum':100},
        'market_regime':{'type':'string','enum':['TREND_WZROSTOWY','TREND_SPADKOWY','KONSOLIDACJA','PRZEJSCIE','NIEJASNY']},
        'signal_alignment':{'type':'string','enum':['ZGODNY_Z_RUCHEM','PRZECIW_RUCHOWI','NEUTRALNY']},
        'reason':{'type':'string'}},
        'required':['decision','score','market_regime','signal_alignment','reason'],
        'additionalProperties':False}
    payload = {'model':AI_MODEL,'instructions':AI_INSTRUCTIONS,
        'input':'Ocen ponizszy sygnal US100:\n'+json.dumps(snapshot,ensure_ascii=False,separators=(',',':')),
        'text':{'format':{'type':'json_schema','name':'us100_signal_evaluation','strict':True,'schema':schema}},
        'max_output_tokens':500}
    r=requests.post('https://api.openai.com/v1/responses',
        headers={'Authorization':f'Bearer {OPENAI_API_KEY}','Content-Type':'application/json'},
        json=payload,timeout=45)
    if not r.ok: raise RuntimeError(f'OpenAI HTTP {r.status_code}: {r.text[:500]}')
    output=''.join(c.get('text','') for item in r.json().get('output',[])
        if item.get('type')=='message' for c in item.get('content',[]) if c.get('type')=='output_text')
    if not output: raise RuntimeError('OpenAI zwrocil pusty output')
    return json.loads(output)

def save_ai_decision(signal,snapshot,result):
    record=dict(timestamp=int(time.time()),time_utc=datetime.now(timezone.utc).isoformat(),
                signal=signal,snapshot=snapshot,ai=result)
    try:
        with state_lock:
            with open(AI_LOG_FILE,'a',encoding='utf-8') as f:
                f.write(json.dumps(record,ensure_ascii=False)+'\n')
    except Exception as exc: print('[AI LOG ERROR]',exc,flush=True)

def analyse_signal_with_ai(signal):
    try:
        snapshot=build_market_snapshot(signal)
        if snapshot is None:
            send_telegram_message('US100 V11 AI OBSERVER\nAI: brak analizy - dane M15/H1 nie sa gotowe.\nENTRY/SL/TP wyslano osobno.')
            return
        observer_state['ai_calls']+=1
        result=call_openai(snapshot)
        observer_state['last_ai_decision']=result
        save_state(); save_ai_decision(signal,snapshot,result)
        send_telegram_message('US100 V11 AI OBSERVER\n\n'
            f'TradingView: {signal["side"]}\n'
            f'ENTRY: {signal["strategy_entry"]:.2f}\n'
            f'SL: {signal["strategy_sl"]:.2f}\n'
            f'TP: {signal["strategy_tp"]:.2f}\n\n'
            f'AI: {result["decision"]}\nOcena: {result["score"]}/100\n'
            f'Rynek: {result["market_regime"]}\nUklad: {result["signal_alignment"]}\n\n'
            f'Powod: {result["reason"]}\n\nTYLKO OBSERWACJA - BRAK TRANSAKCJI')
    except Exception as exc:
        observer_state['ai_errors']+=1
        observer_state['last_ai_decision']={'error':str(exc)}
        save_state()
        print('[AI ERROR]',exc,flush=True)
        send_telegram_message(f'US100 V11 AI OBSERVER\nAI ERROR: {str(exc)[:300]}\nENTRY/SL/TP wyslano osobno.')

def process_strategy_alert(text):
    print('[TV] RAW:',str(text).replace('\n',' | ')[:1000],flush=True)
    signal=parse_strategy_alert(text)
    print('[TV] PARSED:',signal,flush=True)
    if signal['event']!='ENTRY' or signal['side'] not in ('LONG','SHORT'):
        print('[TV] REJECTED: event/side',flush=True); return
    if signal['symbol']!='US100' or signal['timeframe'] not in ('15','15m','15min','m15'):
        print('[TV] REJECTED: symbol/timeframe',flush=True); return
    if any(signal[k] is None or not math.isfinite(signal[k]) for k in
           ('strategy_entry','strategy_sl','strategy_tp')):
        print('[TV] REJECTED: missing/invalid ENTRY SL TP',flush=True); return
    observer_state['signals_received']+=1
    observer_state['last_signal']=signal
    save_state()
    # FIRST: telegram, independent of cTrader market readiness or AI errors.
    delivered=send_tradingview_signal(signal)
    print('[TV] ENTRY/SL/TP TELEGRAM:',delivered,flush=True)
    # SECOND: optional AI analysis.
    analyse_signal_with_ai(signal)

def request_trendbars(client,tf):
    symbol_id=market_state['US100']['symbol_id']
    if not ctrader_state['account_authorized'] or not symbol_id: return
    req=ProtoOAGetTrendbarsReq()
    req.ctidTraderAccountId=int(ctrader_state['account_id']); req.symbolId=int(symbol_id)
    req.period=ProtoOATrendbarPeriod.M15 if tf=='M15' else ProtoOATrendbarPeriod.H1
    now_ms=int(time.time()*1000)
    req.fromTimestamp=now_ms-(35 if tf=='M15' else 30)*86400000
    req.toTimestamp=now_ms
    req.count=1500
    client.send(req).addErrback(safe_errback)

def request_account_data(client):
    if ctrader_state['account_authorized'] and not market_state['US100']['found']:
        req=ProtoOASymbolsListReq(); req.ctidTraderAccountId=int(ctrader_state['account_id'])
        req.includeArchivedSymbols=False
        client.send(req).addErrback(safe_errback)

def refresh_market():
    if ctrader_client is None or not ctrader_state['account_authorized']: return
    if market_state['US100']['digits'] is not None:
        request_trendbars(ctrader_client,'M15'); request_trendbars(ctrader_client,'H1')

def start_refresh_loop():
    global refresh_started
    if refresh_started: return
    refresh_started=True
    def tick():
        refresh_market(); reactor.callLater(AUTO_REFRESH_SECONDS,tick)
    reactor.callLater(5.0,tick)

def send_account_list(client):
    token=ctrader_state.get('access_token')
    if not token: set_error('Brak access_token'); return
    req=ProtoOAGetAccountListByAccessTokenReq(); req.accessToken=token
    client.send(req).addErrback(safe_errback)

def authorize_account(client):
    if not ctrader_state.get('account_id'): return
    req=ProtoOAAccountAuthReq(); req.ctidTraderAccountId=int(ctrader_state['account_id'])
    req.accessToken=ctrader_state['access_token']
    client.send(req).addErrback(safe_errback)

def start_ctrader_connection():
    global ctrader_client
    if ctrader_client is not None and ctrader_state['connected']:
        send_account_list(ctrader_client); return
    ctrader_client=Client(EndPoints.PROTOBUF_DEMO_HOST,EndPoints.PROTOBUF_PORT,TcpProtocol)
    def connected(client):
        ctrader_state['connected']=True; ctrader_state['error']=None
        req=ProtoOAApplicationAuthReq()
        req.clientId=CTRADER_CLIENT_ID; req.clientSecret=CTRADER_CLIENT_SECRET
        client.send(req).addErrback(safe_errback)
    def disconnected(client,reason):
        ctrader_state['connected']=False; ctrader_state['account_authorized']=False
        print('[CTRADER] DISCONNECTED',reason,flush=True)
    def on_message(client,message):
        pt=message.payloadType
        if pt==ProtoOAApplicationAuthRes().payloadType:
            ctrader_state['application_authorized']=True; send_account_list(client)
        elif pt==ProtoOAGetAccountListByAccessTokenRes().payloadType:
            response=Protobuf.extract(message)
            matches=[a for a in response.ctidTraderAccount
                     if int(getattr(a,'traderLogin',0) or 0)==TARGET_TRADER_LOGIN]
            if not matches: set_error(f'Nie znaleziono DEMO login={TARGET_TRADER_LOGIN}'); return
            account=matches[0]
            if bool(getattr(account,'isLive',False)):
                ctrader_state['account_id']=None; ctrader_state['account_authorized']=False
                set_error('LIVE BLOCKED - DEMO ONLY'); return
            ctrader_state['account_id']=int(account.ctidTraderAccountId)
            authorize_account(client)
        elif pt==ProtoOAAccountAuthRes().payloadType:
            ctrader_state['account_authorized']=True; ctrader_state['error']=None
            request_account_data(client); start_refresh_loop()
        elif pt==ProtoOAAccountDisconnectEvent().payloadType:
            ctrader_state['account_authorized']=False
            reactor.callLater(1.0,authorize_account,client)
        elif pt==ProtoOAAccountsTokenInvalidatedEvent().payloadType:
            ctrader_state['account_authorized']=False
            set_error('Token cTrader wygasl - /ctrader/login')
        elif pt==ProtoOASymbolsListRes().payloadType:
            response=Protobuf.extract(message)
            candidates=[s for s in response.symbol if detect_instrument(s.symbolName)=='US100']
            if not candidates: set_error('Nie znaleziono US100'); return
            exact=[s for s in candidates if normalize_symbol(s.symbolName)=='US100']
            symbol=(exact or candidates)[0]
            market_state['US100'].update(found=True,symbol_id=int(symbol.symbolId),symbol_name=symbol.symbolName)
            req=ProtoOASymbolByIdReq(); req.ctidTraderAccountId=int(ctrader_state['account_id'])
            req.symbolId.append(int(symbol.symbolId))
            client.send(req).addErrback(safe_errback)
        elif pt==ProtoOASymbolByIdRes().payloadType:
            response=Protobuf.extract(message)
            for symbol in response.symbol:
                if int(symbol.symbolId)!=int(market_state['US100']['symbol_id']): continue
                market_state['US100']['digits']=int(symbol.digits)
                request_trendbars(client,'M15'); request_trendbars(client,'H1')
        elif pt==ProtoOAGetTrendbarsRes().payloadType:
            response=Protobuf.extract(message)
            if int(response.symbolId)!=int(market_state['US100']['symbol_id'] or 0): return
            tf=('M15' if response.period==ProtoOATrendbarPeriod.M15 else
                'H1' if response.period==ProtoOATrendbarPeriod.H1 else None)
            if tf is None: return
            digits=market_state['US100']['digits']
            if digits is None: return
            candles=sorted((trendbar_to_dict(bar,digits) for bar in response.trendbar),
                           key=lambda c:c['timestamp'])
            with market_lock:
                market_state['US100']['candles'][tf]=candles
                ctrader_state['market_ready']=all(len(market_state['US100']['candles'][t])>=REG_LENGTH
                    for t in ('M15','H1'))
                ctrader_state['last_market_refresh']=int(time.time())
        elif pt==ProtoOAErrorRes().payloadType:
            response=Protobuf.extract(message)
            code=str(response.errorCode); desc=str(response.description)
            if 'ALREADY_LOGGED_IN' in (code+desc):
                if ctrader_state.get('account_id'):
                    ctrader_state['account_authorized']=True; request_account_data(client)
                else:
                    ctrader_state['application_authorized']=True; send_account_list(client)
                return
            set_error(f'{code}: {desc}')
    ctrader_client.setConnectedCallback(connected)
    ctrader_client.setDisconnectedCallback(disconnected)
    ctrader_client.setMessageReceivedCallback(on_message)
    ctrader_client.startService()

def start_reactor():
    global reactor_started
    if reactor_started: return
    reactor_started=True
    reactor.run(installSignalHandlers=False)

def ensure_reactor():
    if not reactor_started:
        threading.Thread(target=start_reactor,daemon=True).start()
    for _ in range(50):
        if getattr(reactor,'running',False): return True
        time.sleep(.1)
    return False

def bootstrap():
    load_state()
    if not ctrader_state.get('access_token'):
        print('[BOOT] Login required: /ctrader/login',flush=True); return
    if ensure_reactor(): reactor.callFromThread(start_ctrader_connection)

@app.route('/')
def home():
    return jsonify(bot=BOT_NAME,status='ONLINE',mode='AI_OBSERVER_ONLY',trading_enabled=False,
        demo_only=True,target_trader_login=TARGET_TRADER_LOGIN,ai_model=AI_MODEL,
        openai_key_present=bool(OPENAI_API_KEY),connected=ctrader_state['connected'],
        account_authorized=ctrader_state['account_authorized'],
        market_ready=ctrader_state['market_ready'],
        last_market_refresh=ctrader_state['last_market_refresh'],observer=observer_state,
        error=ctrader_state['error'])
@app.route('/health')
def health(): return jsonify(status='ok',version='V11_AI_OBSERVER',demo_only=True,trading_enabled=False)
@app.route('/ai/status')
def ai_status():
    return jsonify(openai_key_present=bool(OPENAI_API_KEY),model=AI_MODEL,**observer_state)
@app.route('/market')
def market():
    m15,h1=closed_candles('M15'),closed_candles('H1')
    return jsonify(market_ready=ctrader_state['market_ready'],symbol=market_state['US100']['symbol_name'],
        m15_bars=len(m15),h1_bars=len(h1),M15_regression=regression_channel(m15),
        H1_regression=regression_channel(h1),MONTH_regression=monthly_regression(m15))
@app.route('/telegram/test')
def telegram_test():
    ok=send_telegram_message('US100 V11 AI OBSERVER\nTelegram dziala.\nBRAK HANDLU.')
    return jsonify(status='success' if ok else 'error',telegram_sent=ok)
@app.route('/webhook',methods=['POST'])
def webhook():
    # Fail closed: never allow an unprotected webhook.
    if not WEBHOOK_SECRET: return jsonify(status='error',message='WEBHOOK_SECRET not configured'),503
    if request.args.get('secret')!=WEBHOOK_SECRET:
        return jsonify(status='error',message='invalid secret'),403
    if request.is_json:
        data=request.get_json(silent=True)
        text=(data.get('message') or data.get('text') or json.dumps(data)) if isinstance(data,dict) else str(data or '')
    else: text=request.get_data(as_text=True)
    if not str(text).strip(): return jsonify(status='error',message='empty alert'),400
    threading.Thread(target=process_strategy_alert,args=(text,),daemon=True).start()
    return jsonify(status='accepted',mode='V11_AI_OBSERVER_ONLY',trading=False),200
@app.route('/market/refresh')
def market_refresh():
    if ctrader_client is None or not ctrader_state['account_authorized']:
        return jsonify(status='error',message='cTrader not authorized'),503
    reactor.callFromThread(refresh_market)
    return jsonify(status='refresh_started')
@app.route('/ctrader/login')
def ctrader_login():
    if not all((CTRADER_CLIENT_ID,CTRADER_CLIENT_SECRET,CTRADER_REDIRECT_URI)):
        return jsonify(status='error',message='Missing cTrader configuration'),503
    return redirect('https://id.ctrader.com/my/settings/openapi/grantingaccess/?'+urlencode({
        'client_id':CTRADER_CLIENT_ID,'redirect_uri':CTRADER_REDIRECT_URI,
        'scope':'accounts','product':'web'}))
@app.route('/ctrader/callback')
def ctrader_callback():
    code=request.args.get('code')
    if not code: return jsonify(status='error',message='Brak kodu OAuth'),400
    try:
        response=requests.get('https://openapi.ctrader.com/apps/token',params={
            'grant_type':'authorization_code','code':code,'redirect_uri':CTRADER_REDIRECT_URI,
            'client_id':CTRADER_CLIENT_ID,'client_secret':CTRADER_CLIENT_SECRET},timeout=15)
        response.raise_for_status(); data=response.json()
        ctrader_state['access_token']=data.get('accessToken')
        ctrader_state['refresh_token']=data.get('refreshToken')
        if not ctrader_state['access_token']: raise RuntimeError('No accessToken')
        save_state()
    except Exception as exc:
        set_error(exc); return jsonify(status='error',message='Token error'),500
    if not ensure_reactor(): return jsonify(status='error',message='Reactor not running'),503
    reactor.callFromThread(start_ctrader_connection)
    return jsonify(status='success',route='DEMO',version='V11_AI_OBSERVER',
                   trading_enabled=False,target_trader_login=TARGET_TRADER_LOGIN)

threading.Thread(target=bootstrap,daemon=True).start()
if __name__=='__main__':
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')))
