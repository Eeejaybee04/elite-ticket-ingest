import os, io, re, json
from flask import Flask, request, jsonify
from pdfminer.high_level import extract_text
from datetime import date

# Secret for authentication between Gmail script and this API
APP_SECRET = os.getenv("INGEST_SECRET", "REPLACE_WITH_STRONG_RANDOM_SECRET")
RULES_PATH = os.getenv("RULES_PATH", "rules.json")

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # accept PDFs up to 25 MB

# Tax codes we want to track for PX and CG tickets
CODES = ("YQ", "YR", "GC", "I9", "XT")

def load_rules():
    """Load existing rules.json or return empty dict"""
    if os.path.exists(RULES_PATH):
        with open(RULES_PATH, "r") as f:
            return json.load(f)
    return {}

def save_rules(rules):
    """Save updated rules back to rules.json"""
    with open(RULES_PATH, "w") as f:
        json.dump(rules, f, indent=2, sort_keys=True)

def _money(s: str) -> float:
    try:
        return float(s.replace(",", ""))
    except Exception:
        return 0.0

def parse_ticket_pdf(pdf_bytes: bytes) -> dict:
    """
    Parse a BSP or e-ticket PDF for PX / CG.
    Extract base fare, taxes (YQ/YR/XT/GC/I9), and total.
    """
    text = extract_text(io.BytesIO(pdf_bytes)) or ""
    # Normalize
    text_up = text.upper()
    lines = [re.sub(r'\s+', ' ', L).strip() for L in text_up.splitlines() if L.strip()]

    # ---- carrier ----
    carrier = "UNK"
    if " AIR NIUGINI" in text_up or re.search(r'\bPX\b', text_up): carrier = "PX"
    if " PNG AIR" in text_up or re.search(r'\bCG\b', text_up):    carrier = "CG"

    # ---- currency ----
    currency = "PGK"
    mcur = re.search(r'\b(PGK)\b', text_up)
    if mcur: currency = mcur.group(1)

    # ---- route (prefer real IATA airport codes; avoid words like PNG/AIR/PGK) ----
    BAD = {"PNG","AIR","PGK","TTL","TAX","TOTAL"}
    route = "UNK-UNK"

    # common separators -, /, space
    pairs = re.findall(r'\b([A-Z]{3})\s*[-/ ]\s*([A-Z]{3})\b', text_up)
    # choose the first pair that isn't a BAD word
    for a,b in pairs:
        if a not in BAD and b not in BAD:
            route = f"{a}-{b}"
            break

    # ---- components ----
    CODES = ("YQ","YR","GC","I9","XT")
    components = {c: 0.0 for c in CODES}

    # pass 1: same-line or next-line number after the code
    for i, L in enumerate(lines):
        for c in CODES:
            if re.search(rf'\b{c}\b', L):
                m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
                if m:
                    components[c] = max(components[c], float(m.group(1).replace(',', '')))
                elif i + 1 < len(lines):
                    m2 = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', lines[i+1])
                    if m2:
                        components[c] = max(components[c], float(m2.group(1).replace(',', '')))

    # pass 2: loose formats like "GC 45.60" or "YR: 30.00"
    for c in CODES:
        if components[c] == 0.0:
            m = re.search(rf'\b{c}\b[:\s]*([0-9]{{1,3}}(?:,[0-9]{{3}})*\.[0-9]{{2}})', text_up)
            if m:
                components[c] = float(m.group(1).replace(',', ''))

    # ---- base & total ----
    base = 0.0
    total = 0.0
    for L in lines:
        if re.search(r'\b(BASE\s*FARE|FARE)\b', L):
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m: base = max(base, float(m.group(1).replace(',', '')))
        if re.search(r'\b(TOTAL|TOTAL AMOUNT|TOTAL FARE|GRAND TOTAL)\b', L):
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m: total = max(total, float(m.group(1).replace(',', '')))

    if total == 0.0:
        amts = [float(x.replace(',','')) for x in re.findall(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', text_up)]
        total = max(amts) if amts else 0.0

    return {
        "carrier": carrier,
        "route": route,
        "currency": currency,
        "components": {"base": base, **components},
        "total": total
    }

def upsert_rule(ticket: dict, pos: str = "PG"):
    """
    Use parsed ticket data to update/add a rule in rules.json.
    Key = carrier|route|pos|currency.
    """
    key = f'{ticket["carrier"]}|{ticket["route"]}|{pos}|{ticket["currency"]}'
    rules = load_rules()
    r = rules.get(key, {})

    yqyr = float(ticket["components"].get("YQ", 0)) + float(ticket["components"].get("YR", 0))
    xt    = float(ticket["components"].get("XT", 0))
    gc    = float(ticket["components"].get("GC", 0))
    i9    = float(ticket["components"].get("I9", 0))

    # Update only if we have valid data
    if yqyr: r["yqyr_offset"] = round(yqyr, 2)
    if xt:   r["xt_offset"]   = round(xt, 2)
    if gc:   r["gc_tax"]      = round(gc, 2)
    if i9:   r["i9_tax"]      = round(i9, 2)

    r["last_verified_at"] = date.today().isoformat()
    rules[key] = r
    save_rules(rules)
    return key, r

# ----- ingest endpoint (ONE definition only) -----
@app.post("/ingest-ticket")
def ingest_ticket():
    # auth
    if (request.form.get("secret") or request.headers.get("X-Secret")) != APP_SECRET:
        return jsonify({"error": "unauthorized"}), 401

    # file presence
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "no file"}), 400

    # parse + update rules
    pdf_bytes = f.read()
    ticket = parse_ticket_pdf(pdf_bytes)
    key, rule = upsert_rule(ticket)
    return jsonify({"ok": True, "rule_key": key, "parsed": ticket, "updated_rule": rule})

# ----- test upload form (manual browser test) -----
@app.get("/_test_upload")
def test_form():
    return f"""
      <form action="/ingest-ticket" method="post" enctype="multipart/form-data">
        <input type="hidden" name="secret" value="{APP_SECRET}">
        <input type="file" name="file" accept="application/pdf">
        <button type="submit">Upload PDF</button>
      </form>
    """

# ----- health check -----
@app.get("/")
def health():
    return jsonify({"ok": True, "message": "Elite Ticket Ingest API is running."})
