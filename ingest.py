# pip install ics requests python-dateutil pytz feedparser playwright extruct w3lib
# playwright install
import csv, json, hashlib, datetime as dt, pytz, requests, feedparser
from dateutil import parser as dp
from ics import Calendar
from playwright.sync_api import sync_playwright
import extruct
from w3lib.html import get_base_url

AIRTABLE_WEBHOOK = "https://hooks.airtable.com/workflows/v1/genericWebhook/APP/WORKFLOW/WEBHOOK"  # <-- your URL
TZ = pytz.timezone("America/Denver")

def now_iso():
    return dt.datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()

def iso_date(d):
    if not d: return None
    try:
        return dp.parse(str(d)).astimezone(TZ).date().isoformat()
    except Exception:
        return None

def iso_time(d):
    if not d: return None
    try:
        return dp.parse(str(d)).astimezone(TZ).strftime("%-I:%M %p")
    except Exception:
        return None

def weekday_name(d):
    try:
        d = dp.parse(str(d)).date()
        return d.strftime("%A")
    except Exception:
        return None

def sha1_id(*parts):
    return hashlib.sha1("||".join([p or "" for p in parts]).encode()).hexdigest()

def post_event(data):
    # data: dict with keys matching your generic payload schema
    payload = {"data": data}
    requests.post(AIRTABLE_WEBHOOK, json=payload, timeout=30).raise_for_status()

# ---------- ICS ----------
def ingest_ics(url, source_name, default_location=None):
    txt = requests.get(url, timeout=30).text
    cal = Calendar(txt)
    for e in cal.events:
        start = e.begin.datetime if e.begin else None
        data = {
            "event_name": (e.name or "").strip(),
            "date": iso_date(start),
            "day": weekday_name(start),
            "time": iso_time(start),
            "host_org": source_name,
            "description": (e.description or "").strip() if getattr(e, "description", None) else None,
            "cost": None,
            "tags": [],
            "location": default_location or (e.location or None),
            "address": None,
            "link": getattr(e, "url", None) or url,
            "status": "active",
            "source_id": getattr(e, "uid", None) or f"ics:{sha1_id(url, (e.name or '').strip(), iso_date(start))}",
            "last_seen_at": now_iso(),
        }
        post_event(data)

# ---------- JSON-LD from pages ----------
def extract_jsonld_events(url, html):
    data = extruct.extract(html, base_url=get_base_url(html, url), syntaxes=['json-ld'])['json-ld']
    events = []
    def as_list(x): return x if isinstance(x, list) else [x]
    for block in data:
        types = as_list(block.get("@type", []))
        if "Event" in types:
            events.append(block)
        if block.get("@type") == "ItemList":
            for item in block.get("itemListElement", []):
                ent = item.get("item") or {}
                if isinstance(ent, dict) and ent.get("@type") == "Event":
                    events.append(ent)
    return events

def ingest_page(url, source_name, default_location=None):
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page()
        page.goto(url, timeout=60000)
        page.wait_for_load_state("networkidle")
        html = page.content()
        b.close()
    for ev in extract_jsonld_events(url, html):
        start = ev.get("startDate")
        loc   = ev.get("location") if isinstance(ev.get("location"), dict) else {}
        addr  = loc.get("address") if isinstance(loc.get("address"), dict) else {}
        data = {
            "event_name": (ev.get("name") or "").strip(),
            "date": iso_date(start),
            "day": weekday_name(start),
            "time": iso_time(start),
            "host_org": source_name,
            "description": (ev.get("description") or "").strip() if ev.get("description") else None,
            "cost": None,
            "tags": [],
            "location": loc.get("name") or default_location,
            "address": ", ".join([addr.get("streetAddress",""), addr.get("addressLocality",""), addr.get("addressRegion","")]).strip(", "),
            "link": ev.get("url") or url,
            "status": "active",
            "source_id": ev.get("@id") or ev.get("identifier") or f"jsonld:{sha1_id(url, (ev.get('name') or '').strip(), iso_date(start))}",
            "last_seen_at": now_iso(),
        }
        post_event(data)

# ---------- RSS ----------
def ingest_rss(url, source_name, default_location=None):
    feed = feedparser.parse(url)
    for entry in feed.entries:
        start = entry.get("published") or entry.get("updated")
        data = {
            "event_name": entry.get("title"),
            "date": iso_date(start),
            "day": weekday_name(start),
            "time": iso_time(start),
            "host_org": source_name,
            "description": (entry.get("summary") or "")[:1000],
            "cost": None,
            "tags": [t.get("term") for t in entry.get("tags", []) if t.get("term")] if entry.get("tags") else [],
            "location": default_location,
            "address": None,
            "link": entry.get("link"),
            "status": "active",
            "source_id": f"rss:{sha1_id(url, entry.get('id') or entry.get('link') or entry.get('title'))}",
            "last_seen_at": now_iso(),
        }
        post_event(data)

# ---------- Runner ----------
def run_from_csv(path="sources.csv"):
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            t = (row.get("type") or "").strip().lower()
            url = row.get("url"); name = row.get("source_name") or "Unknown"
            default_loc = row.get("default_location")
            if not url or not t: continue
            try:
                if t == "ics": ingest_ics(url, name, default_loc)
                elif t == "rss": ingest_rss(url, name, default_loc)
                elif t in ("page","jsonld"): ingest_page(url, name, default_loc)
                elif t == "api":
                    # Do a GET and map the API’s event fields into `data` then post_event(data)
                    # (APIs vary—add handlers per API you use)
                    pass
            except Exception as e:
                print("Error on", t, url, e)

if __name__ == "__main__":
    run_from_csv("sources.csv")
