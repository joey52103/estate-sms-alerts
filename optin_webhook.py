import csv
import os
import re
import sqlite3
from datetime import datetime
from typing import Optional

from flask import Flask, request
from dotenv import load_dotenv
import phonenumbers
from twilio.twiml.messaging_response import MessagingResponse

load_dotenv()

app = Flask(__name__)

DEFAULT_REGION = os.getenv("DEFAULT_REGION", "US")

DB_PATH = os.getenv("CONTACTS_DB", "contacts.db")
EXPORT_CSV = os.getenv("CONTACTS_CSV", "contacts.csv")
OPTOUT_FILE = os.getenv("OPTOUT_FILE", "optouts.txt")

# Opt-in keywords
OPTIN_KEYWORDS = {"JOIN", "START", "SUBSCRIBE"}

# Opt-out keywords (carriers send STOP; some people typo STOPA etc.)
OPTOUT_KEYWORDS = {
    "STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT", "STOPA", "STOP1", "STOP2"
}

HELP_KEYWORDS = {"HELP", "INFO"}

# If someone is opted in and we ask for their name, next inbound message becomes their name
ASK_NAME_ON_JOIN = True


# -------------------------
# Helpers
# -------------------------
def normalize_e164(raw: str, default_region: str = "US") -> Optional[str]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        parsed = phonenumbers.parse(raw, None if raw.startswith("+") else default_region)
        if not phonenumbers.is_valid_number(parsed):
            return None
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None


def clean_name(s: str) -> str:
    """
    Keep it simple: letters, spaces, hyphens, apostrophes. Trim length.
    """
    s = (s or "").strip()
    s = re.sub(r"[^a-zA-Z\s'\-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:30]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                phone TEXT PRIMARY KEY,
                name TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'OPTED_IN',   -- OPTED_IN or OPTED_OUT
                pending_name INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def upsert_contact(phone: str, *, name: Optional[str] = None, status: Optional[str] = None,
                   pending_name: Optional[int] = None) -> None:
    now = datetime.utcnow().isoformat()
    with db() as conn:
        cur = conn.execute("SELECT phone, name, status, pending_name, created_at FROM contacts WHERE phone = ?", (phone,))
        row = cur.fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO contacts (phone, name, status, pending_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    phone,
                    name or "",
                    status or "OPTED_IN",
                    int(pending_name or 0),
                    now,
                    now,
                ),
            )
        else:
            old_created = row[4]
            new_name = name if name is not None else row[1]
            new_status = status if status is not None else row[2]
            new_pending = int(pending_name) if pending_name is not None else row[3]

            conn.execute(
                """
                UPDATE contacts
                SET name = ?, status = ?, pending_name = ?, created_at = ?, updated_at = ?
                WHERE phone = ?
                """,
                (new_name, new_status, new_pending, old_created, now, phone),
            )


def get_contact(phone: str) -> Optional[dict]:
    with db() as conn:
        cur = conn.execute(
            "SELECT phone, name, status, pending_name FROM contacts WHERE phone = ?",
            (phone,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"phone": row[0], "name": row[1], "status": row[2], "pending_name": row[3]}


def export_contacts_csv_and_optouts() -> None:
    """
    Exports:
    - contacts.csv with ONLY OPTED_IN contacts: phone,name
    - optouts.txt with OPTED_OUT phones, one per line
    """
    init_db()
    with db() as conn:
        opted_in = conn.execute(
            "SELECT phone, name FROM contacts WHERE status = 'OPTED_IN' ORDER BY updated_at DESC"
        ).fetchall()
        opted_out = conn.execute(
            "SELECT phone FROM contacts WHERE status = 'OPTED_OUT' ORDER BY updated_at DESC"
        ).fetchall()

    # contacts.csv
    with open(EXPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["phone", "name"])
        for phone, name in opted_in:
            w.writerow([phone, name or ""])

    # optouts.txt
    with open(OPTOUT_FILE, "w", encoding="utf-8") as f:
        for (phone,) in opted_out:
            f.write(phone + "\n")


def tokens_upper(body: str) -> set:
    body = (body or "").strip().upper()
    # Split on whitespace for keyword detection
    return set(body.split())


# -------------------------
# Webhook
# -------------------------
@app.route("/sms", methods=["POST"])
def inbound_sms():
    init_db()

    from_number = request.form.get("From", "")
    body = (request.form.get("Body") or "").strip()
    body_upper_tokens = tokens_upper(body)

    phone = normalize_e164(from_number, DEFAULT_REGION)

    resp = MessagingResponse()
    if not phone:
        resp.message("Invalid number. Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    contact = get_contact(phone)

    # 1) OPT-OUT (auto)
    if body_upper_tokens & OPTOUT_KEYWORDS:
        upsert_contact(phone, status="OPTED_OUT", pending_name=0)
        export_contacts_csv_and_optouts()
        print(f"[{datetime.now().isoformat()}] OPTOUT: {phone} body='{body}'")
        resp.message("You’re opted out. Reply START to resubscribe.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    # 2) HELP
    if body_upper_tokens & HELP_KEYWORDS:
        resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    # 3) OPT-IN
    if (body_upper_tokens & OPTIN_KEYWORDS) or (body.strip().upper() in OPTIN_KEYWORDS):
        # If previously opted out, re-opt-in them
        # If new, create them
        need_name = 1 if ASK_NAME_ON_JOIN else 0

        # If they already have a name, no need to ask again
        existing_name = (contact["name"] if contact else "").strip() if contact else ""
        if existing_name:
            need_name = 0

        upsert_contact(phone, status="OPTED_IN", pending_name=need_name)

        export_contacts_csv_and_optouts()
        print(f"[{datetime.now().isoformat()}] OPTIN: {phone} body='{body}' pending_name={need_name}")

        if need_name:
            resp.message("You’re subscribed! Reply with your first name (example: Joey). Reply STOP to opt out.")
        else:
            resp.message("You’re subscribed! Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    # 4) NAME CAPTURE (if we asked for it)
    if contact and contact["status"] == "OPTED_IN" and int(contact["pending_name"]) == 1:
        name = clean_name(body)
        if not name:
            resp.message("Please reply with just your first name (example: Joey). Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

        upsert_contact(phone, name=name, pending_name=0)
        export_contacts_csv_and_optouts()
        print(f"[{datetime.now().isoformat()}] NAME SET: {phone} name='{name}'")

        resp.message(f"Thanks, {name}! You’re all set. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    # 5) Default response
    resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
    return str(resp), 200, {"Content-Type": "application/xml"}


if __name__ == "__main__":
    # For local dev; for "always on", use waitress + service steps below.
    app.run(host="0.0.0.0", port=5000, debug=True)
