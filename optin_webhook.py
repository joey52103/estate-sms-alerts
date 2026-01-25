import csv
import os
import re
import sqlite3
from datetime import datetime
from typing import Optional

from flask import render_template, request, redirect, url_for, session, flash
from functools import wraps
from flask import Flask, request, redirect, url_for, session, render_template_string
from dotenv import load_dotenv
import phonenumbers
from twilio.twiml.messaging_response import MessagingResponse

load_dotenv()

app = Flask(__name__)

# IMPORTANT: set this to a long random value in .env for sessions
# Example: SECRET_KEY=1f5c... (use any long random string)
app.secret_key = os.getenv("SECRET_KEY", "CHANGE_ME_PLEASE")

DEFAULT_REGION = os.getenv("DEFAULT_REGION", "US")

DB_PATH = os.getenv("CONTACTS_DB", "contacts.db")
EXPORT_CSV = os.getenv("CONTACTS_CSV", "contacts.csv")
OPTOUT_FILE = os.getenv("OPTOUT_FILE", "optouts.txt")

ADMIN_USER = os.getenv("ADMIN_USER", "dad")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

OPTIN_KEYWORDS = {"JOIN", "START", "SUBSCRIBE"}
OPTOUT_KEYWORDS = {"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT", "STOPA", "STOP1", "STOP2"}
HELP_KEYWORDS = {"HELP", "INFO"}

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
                (phone, name or "", status or "OPTED_IN", int(pending_name or 0), now, now),
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
    init_db()
    with db() as conn:
        opted_in = conn.execute(
            "SELECT phone, name FROM contacts WHERE status = 'OPTED_IN' ORDER BY updated_at DESC"
        ).fetchall()
        opted_out = conn.execute(
            "SELECT phone FROM contacts WHERE status = 'OPTED_OUT' ORDER BY updated_at DESC"
        ).fetchall()

    with open(EXPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["phone", "name"])
        for phone, name in opted_in:
            w.writerow([phone, name or ""])

    with open(OPTOUT_FILE, "w", encoding="utf-8") as f:
        for (phone,) in opted_out:
            f.write(phone + "\n")


def tokens_upper(body: str) -> set:
    body = (body or "").strip().upper()
    return set(body.split())


def admin_logged_in() -> bool:
    return session.get("admin_authed") is True


# -------------------------
# Admin UI (simple + effective)
# -------------------------
LOGIN_HTML = """
<!doctype html>
<title>Admin Login</title>
<style>
body{font-family:system-ui;margin:40px;max-width:520px}
.card{border:1px solid #ddd;border-radius:14px;padding:18px}
input{width:100%;padding:10px;margin:8px 0;border:1px solid #ccc;border-radius:10px}
button{padding:10px 14px;border:0;border-radius:10px;background:#0b5fff;color:white;width:100%}
.msg{color:#b00020;margin-top:10px}
</style>
<h2>J Maslanka Estates – Admin</h2>
<div class="card">
  <form method="post">
    <label>Username</label>
    <input name="user" autocomplete="username" required />
    <label>Password</label>
    <input name="password" type="password" autocomplete="current-password" required />
    <button type="submit">Sign in</button>
    {% if error %}<div class="msg">{{error}}</div>{% endif %}
  </form>
</div>
"""

ADD_HTML = """
<!doctype html>
<title>Add Contact</title>
<style>
body{font-family:system-ui;margin:40px;max-width:720px}
.card{border:1px solid #ddd;border-radius:14px;padding:18px}
input{width:100%;padding:10px;margin:8px 0;border:1px solid #ccc;border-radius:10px}
button{padding:10px 14px;border:0;border-radius:10px;background:#0b5fff;color:white}
small{color:#555}
.ok{color:#0a7a2f;font-weight:600}
.err{color:#b00020;font-weight:600}
.top{display:flex;justify-content:space-between;align-items:center}
a{color:#0b5fff;text-decoration:none}
</style>

<div class="top">
  <h2>Admin – Add Contact</h2>
  <div><a href="/admin/logout">Logout</a></div>
</div>

<div class="card">
  <form method="post">
    <label>Phone number</label>
    <input name="phone" placeholder="(412) 555-1234" required />
    <label>Name (optional)</label>
    <input name="name" placeholder="Joey" />
    <button type="submit">Add</button>
  </form>

  {% if ok %}<p class="ok">{{ok}}</p>{% endif %}
  {% if error %}<p class="err">{{error}}</p>{% endif %}

  <p><small>Saved to database and exported to contacts.csv automatically.</small></p>
</div>
"""

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if admin_logged_in():
        return redirect(url_for("admin_add"))

    error = None
    if request.method == "POST":
        user = (request.form.get("user") or "").strip()
        pw = (request.form.get("password") or "").strip()

        if user == ADMIN_USER and ADMIN_PASSWORD and pw == ADMIN_PASSWORD:
            session["admin_authed"] = True
            return redirect(url_for("admin_add"))
        error = "Invalid login."

    return render_template_string(LOGIN_HTML, error=error)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@app.route("/admin/add", methods=["GET", "POST"])
def admin_add():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))

    ok = None
    error = None

    if request.method == "POST":
        init_db()
        raw_phone = request.form.get("phone") or ""
        raw_name = request.form.get("name") or ""

        phone = normalize_e164(raw_phone, DEFAULT_REGION)
        name = clean_name(raw_name)

        if not phone:
            error = "That phone number looks invalid. Try again (include area code)."
        else:
            # Manually added contacts should be opted in by your dad’s explicit consent (they wrote it down)
            upsert_contact(phone, name=name, status="OPTED_IN", pending_name=0)
            export_contacts_csv_and_optouts()
            ok = f"Added: {phone}" + (f" ({name})" if name else "")

    return render_template_string(ADD_HTML, ok=ok, error=error)


# -------------------------
# Twilio Webhook
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

    if body_upper_tokens & OPTOUT_KEYWORDS:
        upsert_contact(phone, status="OPTED_OUT", pending_name=0)
        export_contacts_csv_and_optouts()
        resp.message("You’re opted out. Reply START to resubscribe.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if body_upper_tokens & HELP_KEYWORDS:
        resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if body_upper_tokens & OPTIN_KEYWORDS:
        need_name = 1 if ASK_NAME_ON_JOIN else 0
        existing_name = (contact["name"] if contact else "").strip() if contact else ""
        if existing_name:
            need_name = 0

        upsert_contact(phone, status="OPTED_IN", pending_name=need_name)
        export_contacts_csv_and_optouts()

        if need_name:
            resp.message("You’re subscribed! Reply with your first name (example: Joey). Reply STOP to opt out.")
        else:
            resp.message("You’re subscribed! Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if contact and contact["status"] == "OPTED_IN" and int(contact["pending_name"]) == 1:
        name = clean_name(body)
        if not name:
            resp.message("Please reply with just your first name (example: Joey). Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

        upsert_contact(phone, name=name, pending_name=0)
        export_contacts_csv_and_optouts()
        resp.message(f"Thanks, {name}! You’re all set. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
    return str(resp), 200, {"Content-Type": "application/xml"}
