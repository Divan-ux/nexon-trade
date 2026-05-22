import os, json, time, threading, feedparser, re, random
from datetime import datetime, timedelta, timezone
import requests
import yfinance as yf

# ═══ Monkey-patch for Gunicorn + Eventlet ═══
import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS

# ----- NLTK setup (tokenizer) ---------------------------------
import nltk
nltk_data_dir = os.path.join(os.getcwd(), 'nltk_data')
os.makedirs(nltk_data_dir, exist_ok=True)
nltk.data.path.insert(0, nltk_data_dir)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', download_dir=nltk_data_dir, quiet=True)

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
from geotext import GeoText

# ----- App config --------------------------------------------
SECRET_KEY = os.environ.get("SECRET_KEY", "nexon-pulse-prod-secret")
app = Flask(__name__, static_folder='.', template_folder='.')
app.config['SECRET_KEY'] = SECRET_KEY
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# ----- Feeds & tickers ---------------------------------------
RSS_FEEDS = {
    "cybersecurity": [
        "https://feeds.feedburner.com/TheHackersNews",
        "https://krebsonsecurity.com/feed/",
        "https://www.darkreading.com/rss.xml",
        "https://www.bleepingcomputer.com/feed/",
        "https://www.zdnet.com/topic/security/rss.xml",
        "https://www.wired.com/feed/category/security/latest/rss",
        "https://www.cisa.gov/cybersecurity-advisories/rss.xml",
        "https://threatpost.com/feed/",
        "https://www.securityweek.com/rss.xml",
        "https://www.cyberscoop.com/feed/",
        "https://www.infosecurity-magazine.com/feed/",
        "https://www.csoonline.com/feed/",
        "https://www.scmagazine.com/feed/",
        "https://www.helpnetsecurity.com/feed/",
        "https://www.tripwire.com/state-of-security/feed",
        "https://www.schneier.com/blog/atom.xml",
        "https://www.recordedfuture.com/feed",
        "https://www.digitalshadows.com/blog/feed",
        "https://www.fireeye.com/blog/feed",
    ],
    "technology": [
        "https://www.cnbc.com/id/19854910/device/rss/rss.html",
        "https://www.theverge.com/rss/index.xml",
        "https://www.techcrunch.com/feed/",
        "https://www.engadget.com/rss.xml",
        "https://www.wired.com/feed/category/business/latest/rss",
        "https://arstechnica.com/feed/",
        "https://www.technologyreview.com/feed/",
        "https://www.cnet.com/rss/news/",
        "https://www.bbc.com/news/technology/rss.xml",
        "https://www.reuters.com/technology/rss",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/technology/rss.xml",
    ],
    "business_finance": [
        "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "https://www.reuters.com/business/rss",
        "https://www.marketwatch.com/rss/topstories",
        "https://www.ft.com/?format=rss",
        "https://www.economist.com/feeds/print-sections/77/business.xml",
        "https://www.forbes.com/business/feed/",
    ],
    "bloomberg": [
        "https://feeds.bloomberg.com/markets/news.rss",
    ],
    "government": [
        "https://www.cisa.gov/cybersecurity-advisories/rss.xml",
        "https://www.nsa.gov/Press-Room/News-Highlights/Feed/",
        "https://www.fbi.gov/feeds/news/rss.xml",
        "https://www.whitehouse.gov/feed/",
        "https://www.state.gov/feed/",
    ],
    "vulnerability": [
        "https://nvd.nist.gov/feeds/xml/cve/misc/nvd-rss.xml",
        "https://www.exploit-db.com/rss.xml",
        "https://cve.mitre.org/cve/data/updates/rss.xml",
    ],
    "ai_ml": [
        "https://www.artificialintelligence-news.com/feed/",
        "https://www.aitrends.com/feed/",
        "https://machinelearningmastery.com/feed/",
        "https://www.deeplearning.ai/the-batch/feed/",
        "https://www.technologyreview.com/topic/artificial-intelligence/feed/",
    ]
}

STOCK_TICKERS = [
    "MSFT", "GOOGL", "AMZN", "AAPL", "META", "NVDA", "TSLA",
    "CRWD", "PANW", "S", "FTNT", "OKTA", "ZS", "NET", "CHKP", "TENB", "VRNS",
    "QLYS", "RPD", "DDOG", "FSLY", "AKAM", "CLBT", "OSPN", "ATEN", "EVTC",
    "ADBE", "ORCL", "CRM", "NOW", "SNOW", "PLTR", "U", "PATH", "MNDY", "GTLB",
    "DBX", "BOX", "ZM", "DOCN", "SENT", "RBRK",
    "INTC", "AMD", "CSCO", "IBM",
]

FOREX_PAIRS = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X"]

# ----- Global state -------------------------------------------
state_lock = threading.Lock()
live_state = {
    "articles": [],
    "stocks": {},
    "stock_history": {},
    "threats": [],
    "vulns": [],
    "companies": [],
    "briefings": [],
    "ticker_news": [],
    "incidents": 0,
    "last_updated": None,
    "sources_count": 80,
    "forex": {},
    "drop_analysis": {}
}

# ----- Helper functions -------------------------------------
def make_json_safe(obj):
    if isinstance(obj, datetime): return obj.isoformat()
    elif isinstance(obj, dict): return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list): return [make_json_safe(i) for i in obj]
    return obj

def summarize_text(text, sentences=2):
    if not text or len(text) < 100: return text[:200] if text else ""
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LsaSummarizer()
        summary = summarizer(parser.document, sentences)
        return " ".join(str(s) for s in summary)
    except:
        return text[:200] + "..."

def extract_countries(text):
    if not text: return ["Global"]
    try:
        places = GeoText(text)
        countries = list(places.countries)
        return countries if countries else ["Global"]
    except:
        return ["Global"]

def categorize_article(entry, feed_category):
    title = (entry.get("title") or "").lower()
    summary = (entry.get("summary") or "").lower()
    full = title + " " + summary
    if "ransomware" in full: return "Ransomware"
    if "zero-day" in full or "zero day" in full: return "Zero-Day"
    if "phish" in full: return "Phishing"
    if "ddos" in full: return "DDoS"
    if "apt" in full or "nation-state" in full: return "APT"
    if "cloud" in full: return "Cloud"
    if "vulnerability" in full or "cve" in full: return "Vulnerability"
    if "breach" in full or "data leak" in full: return "Breach"
    if "ai" in full or "machine learning" in full: return "AI Threat"
    if feed_category == "cybersecurity": return "Cyber"
    if feed_category == "technology": return "Tech"
    if feed_category == "business_finance": return "Market"
    if feed_category == "bloomberg": return "Bloomberg"
    if feed_category == "government": return "Government"
    return "Other"

# ----- Data fetching (with timeouts) -------------------------
def run_with_timeout(func, timeout=20, default=None, *args, **kwargs):
    result = [default]
    def wrapper():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            print(f"[TIMEOUT] {func.__name__} error: {e}")
    t = threading.Thread(target=wrapper, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        print(f"[TIMEOUT] {func.__name__} timed out after {timeout}s")
    return result[0]

def fetch_all_rss():
    articles = []
    for category, urls in RSS_FEEDS.items():
        for url in urls:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    pub = entry.get("published_parsed") or entry.get("updated_parsed")
                    ts = None
                    if pub: ts = datetime(*pub[:6])
                    articles.append({
                        "id": entry.get("id") or entry.get("link"),
                        "title": entry.get("title", ""),
                        "link": entry.get("link", ""),
                        "summary": entry.get("summary", ""),
                        "published": ts.isoformat() if ts else None,
                        "source": feed.feed.get("title", url),
                        "category": category,
                        "type": categorize_article(entry, category)
                    })
            except: pass
    seen = set()
    unique = []
    for a in articles:
        if a["id"] not in seen:
            seen.add(a["id"])
            unique.append(a)
    unique.sort(key=lambda x: x["published"] or "", reverse=True)
    return unique[:120]

def fetch_stock_quotes():
    quotes = {}
    for sym in STOCK_TICKERS:
        try:
            ticker = yf.Ticker(sym)
            info = ticker.info
            hist = ticker.history(period="5d")
            prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else None
            current = info.get("regularMarketPrice") or info.get("currentPrice")
            change = (current - prev_close) if current and prev_close else None
            quotes[sym] = {
                "price": current,
                "change": round(change, 2) if change else None,
                "name": info.get("shortName", sym),
                "volume": info.get("regularMarketVolume"),
            }
        except:
            quotes[sym] = {"price": None, "change": None, "name": sym}
    return quotes

def generate_stock_history():
    history = {}
    for sym in STOCK_TICKERS[:10]:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="1mo")
            if not hist.empty:
                history[sym] = [round(p, 2) for p in hist['Close'].tolist()]
        except: pass
    return history

def fetch_forex_rates():
    forex = {}
    for pair in FOREX_PAIRS:
        try:
            ticker = yf.Ticker(pair)
            data = ticker.history(period="2d", interval="1h")
            if not data.empty:
                price = data['Close'].iloc[-1]
                forex[pair] = {"price": round(price, 5)}
        except: pass
    return forex

def fetch_nvd_vulns():
    vulns = []
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=7)
        url = "https://services.nvd.nist.gov/rest/json/cves/2.0"
        params = {
            "pubStartDate": start.strftime("%Y-%m-%dT%H:%M:%S.000"),
            "pubEndDate": end.strftime("%Y-%m-%dT%H:%M:%S.000"),
            "resultsPerPage": 20,
            "sort": "publishedDate"
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("vulnerabilities", [])[:15]:
                cve = item.get("cve", {})
                cve_id = cve.get("id", "")
                desc = next((d["value"] for d in cve.get("descriptions", []) if d.get("lang")=="en"), "")
                metrics = cve.get("metrics", {})
                cvss_data = (metrics.get("cvssMetricV31", [{}])[0] if metrics.get("cvssMetricV31") else
                             metrics.get("cvssMetricV30", [{}])[0] if metrics.get("cvssMetricV30") else {}).get("cvssData", {})
                score = cvss_data.get("baseScore", 0)
                sev = cvss_data.get("baseSeverity", "MEDIUM").upper()
                affected_stocks = [s for s in STOCK_TICKERS if s.lower() in desc.lower()]
                vulns.append({
                    "id": cve_id, "score": score, "sev": sev,
                    "product": " ".join(desc.split()[:8]) if desc else cve_id,
                    "vendor": "NVD", "status": "UNPATCHED",
                    "exploited": "exploit" in str(cve).lower(),
                    "age": "Recent", "desc": desc[:300],
                    "affected_stocks": affected_stocks,
                })
    except Exception as e:
        print(f"NVD API error: {e}")
    return vulns

def transform_to_threats(articles):
    threats = []
    for a in articles:
        if a["category"] in ["cybersecurity", "vulnerability", "government"]:
            sev = "CRITICAL" if a["type"] in ["Ransomware","Zero-Day","APT","Breach"] else "HIGH"
            full_text = (a["title"] + " " + (a["summary"] or "")).lower()
            affected = [s for s in STOCK_TICKERS if s.lower() in full_text]
            threats.append({
                "id": a["id"], "type": a["type"], "name": a["title"][:80],
                "sev": sev, "region": extract_countries(a["summary"])[0],
                "industry": "Multiple",
                "ts": a["published"][:16].replace("T"," ") if a.get("published") else "unknown",
                "summary": summarize_text(a["summary"] or "", 1)[:200],
                "source": a["source"], "link": a["link"],
                "affected_stocks": affected,
            })
    return threats[:20]

def generate_briefing(articles):
    top5 = articles[:5]
    summaries = [summarize_text(a["summary"] or a["title"]) for a in top5]
    return {
        "title": "Daily Intelligence Briefing",
        "content": " ".join(summaries),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def generate_company_list():
    companies = []
    for sym in STOCK_TICKERS:
        companies.append({
            "name": sym, "ticker": sym,
            "risk": random.randint(10, 95), "ai": random.randint(30, 100),
            "incidents": random.randint(0, 20),
            "sector": random.choice(["Cybersecurity", "Big Tech", "Cloud", "AI/ML", "Semiconductors"]),
            "status": random.choice(["MONITORED", "CLEAR", "ELEVATED", "HIGH RISK"])
        })
    return companies

def build_stock_drop_analysis(stocks, articles):
    analysis = {}
    for sym, data in stocks.items():
        if data.get("change") is not None and data["change"] < 0:
            related = []
            for a in articles:
                text = (a.get("title","") + " " + a.get("summary","")).lower()
                if sym.lower() in text:
                    related.append(a["title"][:100])
            analysis[sym] = {
                "drop_pct": round(data["change"], 2),
                "related_news": related[:3],
                "risk_level": "HIGH" if abs(data["change"]) > 3 else "MEDIUM"
            }
    return analysis

def generate_dummy_data():
    now = datetime.now(timezone.utc)
    dummy_articles = [
        {"id":f"dummy{i}", "title":f"Sample Cyber Threat {i}", "link":"#",
         "summary":"This is a placeholder article. Real data is loading...",
         "published":now.isoformat(), "source":"Nexon Pulse", "category":"cybersecurity", "type":"Cyber"}
        for i in range(3)
    ]
    return {
        "articles": dummy_articles,
        "threats": [{"id":"t1","name":"Placeholder Threat","sev":"HIGH","region":"Global","type":"Cyber","ts":now.strftime("%Y-%m-%d %H:%M")}],
        "vulns": [],
        "companies": generate_company_list(),
        "briefings": [{"title":"Initializing...","content":"The dashboard is starting up. Real data will appear shortly.","timestamp":now.isoformat()}],
        "ticker_news": [a["title"] for a in dummy_articles],
        "incidents": 1,
        "last_updated": now.isoformat(),
        "sources_count": 80,
        "stocks": {},
        "stock_history": {},
        "forex": {},
        "drop_analysis": {}
    }

# ----- Core refresh ------------------------------------------
def refresh_all():
    global live_state
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Starting refresh...")
    dummy = generate_dummy_data()
    with state_lock:
        live_state.update(dummy)
    socketio.emit("live_data", make_json_safe(live_state))

    def fetch_real():
        articles = run_with_timeout(fetch_all_rss, timeout=20, default=[])
        threats = transform_to_threats(articles) if articles else dummy["threats"]
        ticker_news = [a["title"] for a in articles[:20]] if articles else dummy["ticker_news"]
        briefing = generate_briefing(articles) if articles else dummy["briefings"][0]
        vulns = run_with_timeout(fetch_nvd_vulns, timeout=15, default=[])
        companies = generate_company_list()
        incidents = len(threats)
        with state_lock:
            live_state.update({
                "articles": articles if articles else dummy["articles"],
                "threats": threats,
                "vulns": vulns,
                "companies": companies,
                "briefings": [briefing],
                "ticker_news": ticker_news,
                "incidents": incidents,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "sources_count": sum(len(v) for v in RSS_FEEDS.values()),
            })
        socketio.emit("live_data", make_json_safe(live_state))

        stocks = run_with_timeout(fetch_stock_quotes, timeout=30, default={})
        stock_history = run_with_timeout(generate_stock_history, timeout=20, default={})
        forex = run_with_timeout(fetch_forex_rates, timeout=20, default={})
        drop_analysis = build_stock_drop_analysis(stocks, articles) if stocks else {}
        with state_lock:
            live_state.update({
                "stocks": stocks,
                "stock_history": stock_history,
                "forex": forex,
                "drop_analysis": drop_analysis,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            })
        socketio.emit("stock_update", {"stocks": stocks})
        socketio.emit("forex_update", {"forex": forex})
        print("Real data refresh complete.")

    threading.Thread(target=fetch_real, daemon=True).start()

def stock_updater():
    while True:
        time.sleep(60)
        stocks = run_with_timeout(fetch_stock_quotes, timeout=25, default={})
        with state_lock:
            live_state["stocks"] = stocks
        socketio.emit("stock_update", {"stocks": stocks})

def forex_updater():
    while True:
        time.sleep(60)
        forex = run_with_timeout(fetch_forex_rates, timeout=20, default={})
        with state_lock:
            live_state["forex"] = forex
        socketio.emit("forex_update", {"forex": forex})

def scheduled_full_refresh():
    while True:
        time.sleep(300)
        refresh_all()

# ----- Routes -----------------------------------------------
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/api/state")
def full_state():
    return jsonify(make_json_safe(live_state))

@socketio.on("connect")
def on_connect():
    emit("live_data", make_json_safe(live_state))

@socketio.on("request_refresh")
def on_refresh():
    refresh_all()

# ═══════════════════════════════════════════════════════════════
# START BACKGROUND THREADS AT MODULE LEVEL (NO IF BLOCKS)
# This guarantees execution when Gunicorn imports the module.
# ═══════════════════════════════════════════════════════════════
print("🚀 server.py loaded — starting background threads...")
threading.Thread(target=stock_updater, daemon=True).start()
threading.Thread(target=forex_updater, daemon=True).start()
threading.Thread(target=scheduled_full_refresh, daemon=True).start()
threading.Thread(target=refresh_all, daemon=True).start()
print("🚀 Background threads started.")

# ----- Dev server (only used locally) -----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=False, allow_unsafe_werkzeug=True)
