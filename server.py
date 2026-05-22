import os, json, random, time, threading, feedparser, re, copy
from datetime import datetime, timedelta
import requests
import yfinance as yf
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
from geotext import GeoText

from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS

SECRET_KEY = "nexon_pulse_secret"
app = Flask(__name__, static_folder='.', template_folder='.')
app.config['SECRET_KEY'] = SECRET_KEY
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ─── RSS FEEDS ─────────────────────────────────────────────────────
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
    ],
    "technology": [
        "https://www.cnbc.com/id/19854910/device/rss/rss.html",
        "https://www.theverge.com/rss/index.xml",
        "https://www.techcrunch.com/feed/",
        "https://www.wired.com/feed/category/business/latest/rss",
        "https://arstechnica.com/feed/",
        "https://www.cnet.com/rss/news/",
        "https://www.bbc.com/news/technology/rss.xml",
    ],
    "business_finance": [
        "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "https://www.reuters.com/business/rss",
        "https://www.marketwatch.com/rss/topstories",
        "https://www.forbes.com/business/feed/",
    ],
    "bloomberg": [
        "https://feeds.bloomberg.com/markets/news.rss",
    ],
    "government": [
        "https://www.cisa.gov/cybersecurity-advisories/rss.xml",
        "https://www.fbi.gov/feeds/news/rss.xml",
        "https://www.whitehouse.gov/feed/",
    ],
    "vulnerability": [
        "https://nvd.nist.gov/feeds/xml/cve/misc/nvd-rss.xml",
        "https://www.exploit-db.com/rss.xml",
    ],
}

# ─── STOCK TICKERS ──────────────────────────────────────────────────
STOCK_TICKERS = [
    "MSFT", "GOOGL", "AMZN", "AAPL", "META", "NVDA", "TSLA",
    "CRWD", "PANW", "S", "FTNT", "OKTA", "ZS", "NET", "CHKP", "TENB", "VRNS",
    "QLYS", "RPD", "DDOG", "FSLY", "AKAM", "CLBT", "OSPN", "ATEN", "EVTC",
    "ADBE", "ORCL", "CRM", "NOW", "SNOW", "PLTR", "U", "PATH", "MNDY", "GTLB",
    "DBX", "BOX", "ZM", "DOCN", "SENT", "RBRK",
    "INTC", "AMD", "CSCO", "IBM",
]

# ─── FOREX PAIRS ────────────────────────────────────────────────────
FOREX_PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X",
]

# ─── GLOBAL STATE ───────────────────────────────────────────────────
state_lock = threading.Lock()
live_state = {}

def make_json_safe(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj

def summarize_text(text, sentences=2):
    if not text or len(text) < 100:
        return text[:200] if text else ""
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LsaSummarizer()
        summary = summarizer(parser.document, sentences)
        return " ".join(str(s) for s in summary)
    except:
        return text[:200] + "..."

def extract_countries(text):
    if not text:
        return ["Global"]
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

def fetch_all_rss():
    articles = []
    for category, urls in RSS_FEEDS.items():
        for url in urls:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    pub = entry.get("published_parsed") or entry.get("updated_parsed")
                    ts = None
                    if pub:
                        ts = datetime(*pub[:6])
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
            except:
                pass
    seen = set()
    unique = []
    for a in articles:
        if a["id"] not in seen:
            seen.add(a["id"])
            unique.append(a)
    unique.sort(key=lambda x: x["published"] or "", reverse=True)
    return unique[:100]

def fetch_stock_quotes():
    quotes = {}
    # Batch fetch in groups of 10 to avoid rate limits
    batch_size = 10
    all_tickers = [s for s in STOCK_TICKERS]
    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i:i+batch_size]
        try:
            tickers_obj = yf.Tickers(" ".join(batch))
            for sym in batch:
                try:
                    info = tickers_obj.tickers[sym].info
                    hist = tickers_obj.tickers[sym].history(period="5d")
                    prev_close = None
                    if len(hist) >= 2:
                        prev_close = hist['Close'].iloc[-2]
                    current = info.get("regularMarketPrice") or info.get("currentPrice")
                    change = None
                    change_pct = None
                    if current and prev_close:
                        change = current - prev_close
                        change_pct = (change / prev_close) * 100
                    quotes[sym] = {
                        "price": current,
                        "change": change,
                        "change_pct": change_pct,
                        "name": info.get("shortName", sym),
                        "volume": info.get("regularMarketVolume"),
                    }
                except:
                    quotes[sym] = {"price": None, "change": None, "name": sym}
        except Exception as e:
            for sym in batch:
                quotes[sym] = {"price": None, "change": None, "name": sym}
    return quotes

def generate_stock_history():
    history = {}
    for sym in STOCK_TICKERS:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="1mo")
            if not hist.empty:
                history[sym] = [round(p, 2) for p in hist['Close'].tolist()]
            else:
                history[sym] = []
        except:
            history[sym] = []
    return history

def fetch_forex_rates():
    forex = {}
    for pair in FOREX_PAIRS:
        try:
            ticker = yf.Ticker(pair)
            info = ticker.history(period="1d", interval="1m")
            if not info.empty:
                price = info['Close'].iloc[-1]
                forex[pair] = {"price": round(price, 5)}
            else:
                forex[pair] = {"price": None}
        except:
            forex[pair] = {"price": None}
    return forex

def fetch_nvd_vulns():
    vulns = []
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        url = "https://services.nvd.nist.gov/rest/json/cves/2.0"
        params = {
            "pubStartDate": start.strftime("%Y-%m-%dT%H:%M:%S.000"),
            "pubEndDate": end.strftime("%Y-%m-%dT%H:%M:%S.000"),
            "resultsPerPage": 20,
            "sort": "publishedDate"
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("vulnerabilities", [])[:15]:
                cve = item.get("cve", {})
                cve_id = cve.get("id", "")
                desc = next((d["value"] for d in cve.get("descriptions", []) if d.get("lang")=="en"), "")
                metrics = cve.get("metrics", {})
                cvss_v31 = metrics.get("cvssMetricV31", [{}])[0] if metrics.get("cvssMetricV31") else {}
                cvss_v30 = metrics.get("cvssMetricV30", [{}])[0] if metrics.get("cvssMetricV30") else {}
                cvss_data = cvss_v31.get("cvssData", {}) or cvss_v30.get("cvssData", {})
                score = cvss_data.get("baseScore", 0)
                sev = cvss_data.get("baseSeverity", "MEDIUM").upper()
                # Check if any tracked stock is mentioned
                affected_stocks = [s for s in STOCK_TICKERS if s.lower() in desc.lower()]
                vulns.append({
                    "id": cve_id,
                    "score": score,
                    "sev": sev,
                    "product": " ".join(desc.split()[:8]) if desc else cve_id,
                    "vendor": "NVD",
                    "status": "UNPATCHED",
                    "exploited": "exploit" in str(cve).lower(),
                    "age": "Recent",
                    "desc": desc[:300],
                    "affected_stocks": affected_stocks,
                })
    except Exception as e:
        print(f"NVD API error: {e}")
    return vulns

def generate_briefing(articles):
    top5 = articles[:5]
    summaries = [summarize_text(a["summary"] or a["title"]) for a in top5]
    combined = " ".join(summaries)
    return {
        "title": "Daily Intelligence Briefing",
        "content": combined,
        "timestamp": datetime.utcnow().isoformat()
    }

def transform_to_threats(articles):
    threats = []
    for a in articles:
        if a["category"] in ["cybersecurity", "vulnerability", "government"]:
            sev = "CRITICAL" if a["type"] in ["Ransomware","Zero-Day","APT","Breach"] else "HIGH"
            # Find affected stocks
            full_text = (a["title"] + " " + (a["summary"] or "")).lower()
            affected = [s for s in STOCK_TICKERS if s.lower() in full_text]
            threats.append({
                "id": a["id"],
                "type": a["type"],
                "name": a["title"][:80],
                "sev": sev,
                "region": extract_countries(a["summary"])[0],
                "industry": "Multiple",
                "ts": a["published"][:16].replace("T"," ") if a.get("published") else "unknown",
                "summary": summarize_text(a["summary"] or "", 1)[:200],
                "source": a["source"],
                "link": a["link"],
                "affected_stocks": affected,
            })
    return threats[:20]

def generate_company_list():
    companies = []
    sectors_pool = ["Cybersecurity", "Big Tech", "Cloud", "AI/ML", "Semiconductors", "Software", "IT Services"]
    statuses = ["MONITORED", "CLEAR", "ELEVATED", "HIGH RISK"]
    for sym in STOCK_TICKERS:
        companies.append({
            "name": sym,
            "ticker": sym,
            "risk": random.randint(10, 95),
            "ai": random.randint(30, 100),
            "incidents": random.randint(0, 20),
            "sector": random.choice(sectors_pool),
            "status": random.choice(statuses)
        })
    return companies

def build_stock_drop_analysis(stocks, articles):
    """Analyze why stocks dropped based on news"""
    analysis = {}
    for sym, data in stocks.items():
        if data.get("change") is not None and data["change"] < 0:
            # Find articles mentioning this stock
            related = []
            for a in articles:
                text = (a.get("title","") + " " + a.get("summary","")).lower()
                if sym.lower() in text:
                    related.append(a["title"][:100])
            analysis[sym] = {
                "drop_pct": round(data["change_pct"], 2) if data.get("change_pct") else round(data["change"], 2),
                "related_news": related[:3],
                "risk_level": "HIGH" if abs(data.get("change", 0)) > 3 else "MEDIUM" if abs(data.get("change", 0)) > 1 else "LOW"
            }
    return analysis

def refresh_all():
    global live_state
    with state_lock:
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Refreshing data...")
        articles = fetch_all_rss()
        stocks = fetch_stock_quotes()
        stock_history = generate_stock_history()
        threats = transform_to_threats(articles)
        briefing = generate_briefing(articles)
        ticker_news = [a["title"] for a in articles[:20]]
        vulns = fetch_nvd_vulns()
        companies = generate_company_list()
        forex = fetch_forex_rates()
        drop_analysis = build_stock_drop_analysis(stocks, articles)

        new_state = {
            "articles": articles,
            "stocks": stocks,
            "stock_history": stock_history,
            "threats": threats,
            "vulns": vulns,
            "companies": companies,
            "briefings": [briefing],
            "ticker_news": ticker_news,
            "incidents": len(threats),
            "last_updated": datetime.utcnow().isoformat(),
            "sources_count": sum(len(v) for v in RSS_FEEDS.values()),
            "forex": forex,
            "drop_analysis": drop_analysis,
        }
        live_state = make_json_safe(new_state)
    socketio.emit("live_data", live_state)

def stock_updater():
    while True:
        time.sleep(30)
        try:
            stocks = fetch_stock_quotes()
            with state_lock:
                live_state["stocks"] = stocks
            socketio.emit("stock_update", {"stocks": stocks})
        except:
            pass

def forex_updater():
    while True:
        time.sleep(60)
        try:
            forex = fetch_forex_rates()
            with state_lock:
                live_state["forex"] = forex
            socketio.emit("forex_update", {"forex": forex})
        except:
            pass

# ─── ROUTES ────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/api/state")
def full_state():
    return jsonify(live_state)

@socketio.on("connect")
def on_connect(auth=None):
    emit("live_data", live_state)

@socketio.on("request_refresh")
def on_refresh(auth=None):
    refresh_all()

if __name__ == "__main__":
    print("NEXON PULSE SERVER – launching on port 5000")
    refresh_all()
    threading.Thread(target=stock_updater, daemon=True).start()
    threading.Thread(target=forex_updater, daemon=True).start()
    def scheduled():
        while True:
            time.sleep(300)
            refresh_all()
    threading.Thread(target=scheduled, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
