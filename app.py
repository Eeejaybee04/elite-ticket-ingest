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
    Parse a BSP or e-ticket PDF for PX/CG and extract:
    - carrier (PX/CG)
    - route (e.g., POM-LAE) using real IATA airport codes
    - currency (default PGK)
    - components: base, YQ, YR, XT, GC, I9
    - total
    """
    text = extract_text(io.BytesIO(pdf_bytes)) or ""
    T = text.upper()
    lines = [re.sub(r'\s+', ' ', L).strip() for L in T.splitlines() if L.strip()]

    # -------- carrier --------
    carrier = "UNK"
    if " AIR NIUGINI" in T or re.search(r'\bPX\b', T): carrier = "PX"
    if " PNG AIR" in T or re.search(r'\bCG\b', T):      carrier = "CG"

    # -------- currency --------
    currency = "PGK"
    mcur = re.search(r'\b(PGK)\b', T)
    if mcur: currency = mcur.group(1)

    # -------- route (strict: only valid IATA airports) --------
    # Common PNG IATA airport codes (add more as you encounter them)
    PNG_CODES = {
        "POM","LAE","HGU","RAB","GUR","WWK","HKN","MAG","KVG","LSA","TIZ","KIE",
        "LNV","BUA","KRI","KMA","GKA","MDU","TBG","UAK","KRU","HKN","KPX","KDU",
        "RBP","BUL","KSB","KVG","PNP"  # PNP=Girua/Popondetta
    }
    # Neighbours that sometimes appear on regional tickets:
    NEAR = {"BNE","CNS","BNE","CNS","TSV","CNS","BNE","CNS","BNE","CNS","BNE","CNS",
            "CNS","BNE","BNE","CNS","TSV","CNS","HKG","SIN","BKK","MNL","SYD","BNE","CNS","MEL","BNE","CNS"}
    IATA = PNG_CODES | NEAR

    route = "UNK-UNK"

    # pattern 1: FROM XXX TO YYY (allow a few characters in between)
    mft = re.search(r'\bFROM\s+([A-Z]{3})\b.{0,15}\bTO\s+([A-Z]{3})\b', T)
    if mft and mft.group(1) in IATA and mft.group(2) in IATA:
        route = f"{mft.group(1)}-{mft.group(2)}"
    else:
        # pattern 2: XXX-YYY or XXX / YYY or XXX YYY on one line
        pairs = re.findall(r'\b([A-Z]{3})\s*[-/ ]\s*([A-Z]{3})\b', T)
        for a, b in pairs:
            if a in IATA and b in IATA:
                route = f"{a}-{b}"
                break

    # -------- components --------
    CODES = ("YQ","YR","XT","GC","I9")
    components = {c: 0.0 for c in CODES}

    # pass 1: tight – code followed by amount within ~15 chars
    for c in CODES:
        m = re.search(rf'\b{c}\b[^\d]{{0,15}}([0-9]{{1,3}}(?:,[0-9]{{3}})*\.[0-9]{{2}})', T)
        if m:
            components[c] = float(m.group(1).replace(',', ''))

    # pass 2: loose “CODE 45.60” or “CODE: 45.60” anywhere
    for c in CODES:
        if components[c] == 0.0:
            for m in re.finditer(rf'\b{c}\b[:\s]+([0-9]{{1,3}}(?:,[0-9]{{3}})*\.[0-9]{{2}})', T):
                components[c] = max(components[c], float(m.group(1).replace(',', '')))

    # -------- base fare --------
    base = 0.0
    # “BASE FARE … 123.45” or “FARE: PGK 123.45”
    for L in lines:
        if "BASE FARE" in L or re.search(r'\bFARE\b', L):
            m = re.search(r'(?:PGK\s*)?([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m:
                base = max(base, float(m.group(1).replace(',', '')))

    # -------- total --------
    total = 0.0
    for L in lines:
        if re.search(r'\b(GRAND\s+TOTAL|TOTAL\s+AMOUNT|TOTAL\s+FARE|TOTAL)\b', L):
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m:
                total = max(total, float(m.group(1).replace(',', '')))
    if total == 0.0:
        amts = [float(x.replace(',', '')) for x in re.findall(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', T)]
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
# --- dump raw text seen by pdfminer (for debugging) ---
@app.route("/_dump_text", methods=["GET", "POST"])
def dump_text():
    # Show an upload form on GET
    if request.method == "GET":
        return f"""
          <form action="/_dump_text" method="post" enctype="multipart/form-data">
            <input type="hidden" name="secret" value="{APP_SECRET}">
            <input type="file" name="file" accept="application/pdf">
            <button type="submit">Dump PDF Text</button>
          </form>
        """

    # Handle POST: return first 10k chars of extracted text
    if (request.form.get("secret") or request.headers.get("X-Secret")) != APP_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "no file"}), 400
    txt = extract_text(io.BytesIO(f.read())) or ""
    return jsonify({"text": txt[:10000]})


def health():
    return jsonify({"ok": True, "message": "Elite Ticket Ingest API is running."})
