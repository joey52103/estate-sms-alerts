import csv
import json
import os
import re
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
from flask import (
    Flask,
    request,
    redirect,
    url_for,
    session,
    render_template_string,
    jsonify,
    Response,
)
import phonenumbers
from twilio.twiml.messaging_response import MessagingResponse

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "CHANGE_ME_PLEASE")

DEFAULT_REGION = os.getenv("DEFAULT_REGION", "US")
DB_PATH = os.getenv("DB_PATH", "contacts.db")
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
def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


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
    return s[:40]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    # Matches YOUR current schema (contacts)
    # id INTEGER PK, phone TEXT NOT NULL UNIQUE, name TEXT, opted_out INTEGER NOT NULL DEFAULT 0,
    # created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL UNIQUE,
                name TEXT DEFAULT '',
                opted_out INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        # NEW: audit log
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                contact_id INTEGER,
                before_json TEXT,
                after_json TEXT,
                ip TEXT,
                created_at TEXT NOT NULL
            )
            """
        )


def admin_logged_in() -> bool:
    return session.get("admin_authed") is True


def require_admin():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))
    return None


def current_actor() -> str:
    # stored at login
    return (session.get("admin_user") or ADMIN_USER or "admin").strip()


def client_ip() -> str:
    # if behind CF/nginx, you may see X-Forwarded-For; keep simple + safe
    xff = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return xff or (request.remote_addr or "")


def audit_log(actor: str, action: str, contact_id: Optional[int], before: Optional[dict], after: Optional[dict]) -> None:
    init_db()
    now = utc_now()
    before_json = json.dumps(before, separators=(",", ":"), ensure_ascii=False) if before else None
    after_json = json.dumps(after, separators=(",", ":"), ensure_ascii=False) if after else None
    ip = client_ip()
    with db() as conn:
        conn.execute(
            """
            INSERT INTO audit_log (actor, action, contact_id, before_json, after_json, ip, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (actor, action, contact_id, before_json, after_json, ip, now),
        )


def get_contact_by_phone(phone: str) -> Optional[dict]:
    init_db()
    with db() as conn:
        row = conn.execute(
            "SELECT id, phone, name, opted_out, created_at, updated_at FROM contacts WHERE phone = ?",
            (phone,),
        ).fetchone()
        return dict(row) if row else None


def get_contact_by_id(contact_id: int) -> Optional[dict]:
    init_db()
    with db() as conn:
        row = conn.execute(
            "SELECT id, phone, name, opted_out, created_at, updated_at FROM contacts WHERE id = ?",
            (contact_id,),
        ).fetchone()
        return dict(row) if row else None


def add_contact(phone: str, name: str = "", *, actor: Optional[str] = None, log: bool = True) -> bool:
    """Returns True if added, False if already exists."""
    init_db()
    now = utc_now()
    try:
        with db() as conn:
            conn.execute(
                """
                INSERT INTO contacts (phone, name, opted_out, created_at, updated_at)
                VALUES (?, ?, 0, ?, ?)
                """,
                (phone, name, now, now),
            )
        if log:
            c = get_contact_by_phone(phone)
            audit_log(actor or "system", "create", (c or {}).get("id"), None, c)
        return True
    except sqlite3.IntegrityError:
        return False


def set_opted_out(phone: str, opted_out: int, *, actor: Optional[str] = None, log: bool = True) -> None:
    init_db()
    before = get_contact_by_phone(phone)
    with db() as conn:
        conn.execute(
            "UPDATE contacts SET opted_out = ?, updated_at = ? WHERE phone = ?",
            (1 if opted_out else 0, utc_now(), phone),
        )
    after = get_contact_by_phone(phone)
    if log and after:
        audit_log(actor or "system", "opt_out" if int(opted_out) == 1 else "opt_in", after.get("id"), before, after)


def delete_contact(phone: str, *, actor: Optional[str] = None, log: bool = True) -> None:
    init_db()
    before = get_contact_by_phone(phone)
    with db() as conn:
        conn.execute("DELETE FROM contacts WHERE phone = ?", (phone,))
    if log and before:
        audit_log(actor or "system", "delete", before.get("id"), before, None)


def delete_contact_by_id(contact_id: int, *, actor: Optional[str] = None, log: bool = True) -> bool:
    init_db()
    before = get_contact_by_id(contact_id)
    if not before:
        return False
    with db() as conn:
        conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    if log:
        audit_log(actor or "system", "delete", contact_id, before, None)
    return True


def update_contact(old_phone: str, new_phone: str, new_name: str) -> Optional[str]:
    """
    Updates phone and name. Returns error string or None on success.
    (kept for compatibility with your existing edit page + sms name capture)
    """
    init_db()
    old = get_contact_by_phone(old_phone)
    if not old:
        return "Contact not found."

    if new_phone != old_phone and get_contact_by_phone(new_phone):
        return "That phone number already exists."

    with db() as conn:
        if new_phone != old_phone:
            conn.execute(
                """
                UPDATE contacts
                SET phone = ?, name = ?, updated_at = ?
                WHERE phone = ?
                """,
                (new_phone, new_name, utc_now(), old_phone),
            )
        else:
            conn.execute(
                """
                UPDATE contacts
                SET name = ?, updated_at = ?
                WHERE phone = ?
                """,
                (new_name, utc_now(), old_phone),
            )
    return None


def update_contact_by_id(contact_id: int, new_phone: str, new_name: str, opted_out: int, *, actor: str) -> dict:
    init_db()

    before = get_contact_by_id(contact_id)
    if not before:
        raise ValueError("Contact not found.")

    # If phone is changing, enforce uniqueness
    if new_phone != before["phone"]:
        existing = get_contact_by_phone(new_phone)
        if existing and int(existing["id"]) != int(contact_id):
            raise ValueError("That phone number already exists.")

    with db() as conn:
        conn.execute(
            """
            UPDATE contacts
            SET phone = ?, name = ?, opted_out = ?, updated_at = ?
            WHERE id = ?
            """,
            (new_phone, new_name, 1 if opted_out else 0, utc_now(), contact_id),
        )

    after = get_contact_by_id(contact_id)
    audit_log(actor, "update", contact_id, before, after)
    return after


def list_contacts(q: str = "") -> List[Dict]:
    init_db()
    q = (q or "").strip()
    with db() as conn:
        if q:
            like = f"%{q}%"
            rows = conn.execute(
                """
                SELECT id, phone, name, opted_out, created_at, updated_at
                FROM contacts
                WHERE phone LIKE ? OR name LIKE ?
                ORDER BY updated_at DESC
                """,
                (like, like),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, phone, name, opted_out, created_at, updated_at
                FROM contacts
                ORDER BY updated_at DESC
                """
            ).fetchall()
    return [dict(r) for r in rows]


def get_counts() -> Dict[str, int]:
    init_db()
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM contacts").fetchone()["c"]
        opted_in = conn.execute("SELECT COUNT(*) AS c FROM contacts WHERE opted_out = 0").fetchone()["c"]
        opted_out = conn.execute("SELECT COUNT(*) AS c FROM contacts WHERE opted_out = 1").fetchone()["c"]
    return {"total": int(total), "opted_in": int(opted_in), "opted_out": int(opted_out)}


def export_contacts_csv_and_optouts() -> None:
    init_db()
    with db() as conn:
        opted_in = conn.execute(
            "SELECT phone, name FROM contacts WHERE opted_out = 0 ORDER BY updated_at DESC"
        ).fetchall()
        opted_out = conn.execute(
            "SELECT phone FROM contacts WHERE opted_out = 1 ORDER BY updated_at DESC"
        ).fetchall()

    with open(EXPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["phone", "name"])
        for row in opted_in:
            w.writerow([row["phone"], row["name"] or ""])

    with open(OPTOUT_FILE, "w", encoding="utf-8") as f:
        for row in opted_out:
            f.write(row["phone"] + "\n")


def tokens_upper(body: str) -> set:
    body = (body or "").strip().upper()
    return set(body.split())


# -------------------------
# Admin UI
# -------------------------
BASE_HTML = """
<!doctype html>
<html>
<head>
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{font-family:system-ui;margin:40px;max-width:1040px}
    .card{border:1px solid #ddd;border-radius:14px;padding:18px}
    input,select{padding:10px;margin:6px 0;border:1px solid #ccc;border-radius:10px;width:100%}
    button{padding:10px 14px;border:0;border-radius:10px;background:#0b5fff;color:white;cursor:pointer}
    .btn2{background:#555}
    .btnGhost{background:#f2f3f5;color:#222}
    .btnDanger{background:#b00020}
    .ok{color:#0a7a2f;font-weight:700}
    .err{color:#b00020;font-weight:700}
    a{color:#0b5fff;text-decoration:none}
    .nav{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;padding-bottom:12px;border-bottom:1px solid #eee;gap:14px}
    .navleft{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
    .navleft a{font-weight:700}
    .navright{display:flex;align-items:center;gap:14px}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #ddd;padding:10px;text-align:left;vertical-align:top}
    th{background:#f7f7f7}
    .rowActions{white-space:nowrap}
    .rowActions form{display:inline}
    .pill{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px}
    .in{background:#e9f7ef;color:#0a7a2f}
    .out{background:#fdecea;color:#b00020}
    .muted{color:#666}
    .searchRow{display:flex;gap:10px;align-items:center;margin:10px 0 18px}
    .searchRow input{flex:1}
    .actionsTop{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin:10px 0 14px}
    .actionsTop .left{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
    .small{font-size:12px}
    .badge{display:inline-block;padding:2px 10px;border-radius:999px;background:#eef2ff;color:#1e40af;font-weight:800;font-size:12px}
    .kbd{font-family:ui-monospace, SFMono-Regular, Menlo, monospace;background:#f2f3f5;border-radius:8px;padding:2px 6px;font-size:12px}

    /* Inline edit UI */
    .cellLine{display:flex;gap:10px;align-items:flex-start;flex-wrap:wrap}
    .cellLine .col{min-width:240px}
    .inlineField{display:none}
    .editing .inlineView{display:none}
    .editing .inlineField{display:block}
    .miniInput{width:260px}
    .miniSelect{width:180px}

    /* Modal */
    .modalOverlay{position:fixed;inset:0;background:rgba(0,0,0,.45);display:none;align-items:center;justify-content:center;z-index:9999;padding:16px}
    .modal{background:white;border-radius:16px;max-width:520px;width:100%;border:1px solid #e5e7eb;box-shadow:0 20px 60px rgba(0,0,0,.25);padding:18px}
    .modal h3{margin:0 0 6px}
    .modal .row{display:flex;gap:10px;justify-content:flex-end;margin-top:14px}
    .toast{position:fixed;bottom:16px;left:16px;background:#111827;color:white;padding:10px 12px;border-radius:12px;display:none;z-index:10000}
  </style>
</head>
<body>
  {% if show_nav %}
  <div class="nav">
    <div class="navleft">
      <a href="/admin/add">Add Contact</a>
      <a href="/admin/contacts">Contacts <span class="badge">{{ counts.total }}</span></a>
      <a href="/admin/audit">Audit Log</a>
      <span class="muted small">In: <strong>{{ counts.opted_in }}</strong> · Out: <strong>{{ counts.opted_out }}</strong></span>
    </div>
    <div class="navright">
      <span class="muted small">Signed in as <strong>{{ actor }}</strong></span>
      <a href="/admin/logout">Logout</a>
    </div>
  </div>
  {% endif %}

  <div id="toast" class="toast"></div>

  {{ body|safe }}

  <script>
    function showToast(msg){
      const t = document.getElementById('toast');
      if(!t) return;
      t.textContent = msg;
      t.style.display='block';
      clearTimeout(window.__toastTimer);
      window.__toastTimer = setTimeout(()=>{ t.style.display='none'; }, 2400);
    }
  </script>
</body>
</html>
"""


def render_admin(title: str, body: str, *, show_nav: bool = True) -> str:
    counts = get_counts() if show_nav else {"total": 0, "opted_in": 0, "opted_out": 0}
    actor = current_actor() if show_nav else ""
    return render_template_string(BASE_HTML, title=title, body=body, show_nav=show_nav, counts=counts, actor=actor)


@app.route("/")
def home():
    return redirect(url_for("admin_login"))


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if admin_logged_in():
        return redirect(url_for("admin_add"))

    error = ""
    if request.method == "POST":
        user = (request.form.get("user") or "").strip()
        pw = (request.form.get("password") or "").strip()

        # IMPORTANT: ADMIN_PASSWORD must be set (non-empty) in .env
        if user == ADMIN_USER and ADMIN_PASSWORD and pw == ADMIN_PASSWORD:
            session["admin_authed"] = True
            session["admin_user"] = user
            return redirect(url_for("admin_add"))
        error = "Invalid login."

    body = f"""
    <h2>J Maslanka Estates – Admin</h2>
    <div class="card" style="max-width:520px">
      <form method="post">
        <label>Username</label>
        <input name="user" autocomplete="username" required />
        <label>Password</label>
        <input name="password" type="password" autocomplete="current-password" required />
        <button type="submit" style="width:100%">Sign in</button>
        {"<div class='err' style='margin-top:10px'>" + error + "</div>" if error else ""}
      </form>
      <p class="muted small" style="margin-top:10px">Tip: set <span class="kbd">ADMIN_PASSWORD</span> in your <span class="kbd">.env</span>.</p>
    </div>
    """
    return render_admin("Admin Login", body, show_nav=False)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


# -------------------------
# NEW: Admin API endpoints
# -------------------------
@app.route("/admin/api/contacts/exists", methods=["GET"])
def admin_api_contacts_exists():
    gate = require_admin()
    if gate:
        return gate

    raw_phone = request.args.get("phone") or ""
    phone = normalize_e164(raw_phone, DEFAULT_REGION)
    if not phone:
        return jsonify({"ok": True, "valid": False, "exists": False})

    c = get_contact_by_phone(phone)
    return jsonify(
        {
            "ok": True,
            "valid": True,
            "exists": bool(c),
            "name": (c or {}).get("name", ""),
            "id": (c or {}).get("id"),
        }
    )


@app.route("/admin/api/contacts/<int:contact_id>", methods=["POST"])
def admin_api_update_contact(contact_id: int):
    gate = require_admin()
    if gate:
        return gate

    payload = request.get_json(silent=True) or {}
    raw_phone = (payload.get("phone") or "").strip()
    raw_name = (payload.get("name") or "").strip()
    opted_out_raw = payload.get("opted_out")

    phone = normalize_e164(raw_phone, DEFAULT_REGION)
    if not phone:
        return jsonify({"ok": False, "error": "Invalid phone number."}), 400

    name = clean_name(raw_name)
    opted_out = 1 if str(opted_out_raw).strip() in {"1", "true", "True"} else 0

    try:
        updated = update_contact_by_id(contact_id, phone, name, opted_out, actor=current_actor())
        export_contacts_csv_and_optouts()
        return jsonify({"ok": True, "contact": updated})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception:
        return jsonify({"ok": False, "error": "Update failed."}), 500


@app.route("/admin/api/contacts/<int:contact_id>/delete", methods=["POST"])
def admin_api_delete_contact(contact_id: int):
    gate = require_admin()
    if gate:
        return gate

    ok = delete_contact_by_id(contact_id, actor=current_actor(), log=True)
    if ok:
        export_contacts_csv_and_optouts()
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Contact not found."}), 404


# -------------------------
# Admin pages
# -------------------------
@app.route("/admin/add", methods=["GET", "POST"])
def admin_add():
    gate = require_admin()
    if gate:
        return gate

    ok = ""
    error = ""

    if request.method == "POST":
        raw_phone = request.form.get("phone") or ""
        raw_name = request.form.get("name") or ""

        phone = normalize_e164(raw_phone, DEFAULT_REGION)
        name = clean_name(raw_name)

        if not phone:
            error = "That phone number looks invalid. Try again (include area code)."
        else:
            added = add_contact(phone, name, actor=current_actor(), log=True)
            if not added:
                error = "Number already added."
            else:
                export_contacts_csv_and_optouts()
                ok = f"Added: {phone}" + (f" ({name})" if name else "")

    body = f"""
    <h2>Admin – Add Contact</h2>
    <div class="card" style="max-width:720px">
      <form method="post" id="addForm">
        <label>Phone number</label>
        <input id="phoneInput" name="phone" placeholder="(412) 555-1234" required />
        <div id="phoneLiveMsg" class="small muted" style="margin-top:2px"></div>

        <label style="margin-top:10px">Name (optional)</label>
        <input name="name" placeholder="Joey" />

        <button id="addBtn" type="submit">Add</button>
      </form>

      {"<p class='ok'>" + ok + "</p>" if ok else ""}
      {"<p class='err'>" + error + "</p>" if error else ""}

      <p class="muted"><small>Saved to database and exported to contacts.csv automatically.</small></p>
    </div>

    <script>
      (function(){
        const phoneInput = document.getElementById('phoneInput');
        const msg = document.getElementById('phoneLiveMsg');
        const btn = document.getElementById('addBtn');
        let t = null;

        function setState(text, isError){
          msg.textContent = text || '';
          msg.className = 'small ' + (isError ? 'err' : 'muted');
        }

        async function check(){
          const v = (phoneInput.value || '').trim();
          if(!v){ btn.disabled = false; setState('', false); return; }

          try{
            const res = await fetch('/admin/api/contacts/exists?phone=' + encodeURIComponent(v), {credentials:'same-origin'});
            const data = await res.json();
            if(!data.valid){
              btn.disabled = true;
              setState('Invalid number (include area code).', true);
              return;
            }
            if(data.exists){
              btn.disabled = true;
              setState('Already exists' + (data.name ? (' (name: ' + data.name + ')') : '') + '.', true);
              return;
            }
            btn.disabled = false;
            setState('Looks good.', false);
          }catch(e){
            btn.disabled = false;
            setState('', false);
          }
        }

        phoneInput.addEventListener('input', ()=>{
          clearTimeout(t);
          t = setTimeout(check, 420);
        });
      })();
    </script>
    """
    return render_admin("Add Contact", body)


@app.route("/admin/contacts", methods=["GET"])
def admin_contacts():
    gate = require_admin()
    if gate:
        return gate

    q = request.args.get("q", "") or ""
    contacts = list_contacts(q=q)

    # Export link respects search query
    export_link = "/admin/contacts/export.csv" + (f"?q={q}" if q else "")

    rows_html = ""
    for c in contacts:
        cid = int(c["id"])
        phone = c["phone"]
        name = (c["name"] or "").strip()
        opted_out = int(c["opted_out"]) == 1
        updated = c["updated_at"]

        pill = "<span class='pill out'>OPTED OUT</span>" if opted_out else "<span class='pill in'>OPTED IN</span>"

        rows_html += f"""
        <tr id="row-{cid}" data-id="{cid}">
          <td>
            <div class="cellLine">
              <div class="col">
                <div class="inlineView">
                  <strong class="v-phone">{phone}</strong><br>
                  <span class="muted v-name">{name}</span>
                </div>

                <div class="inlineField">
                  <label class="small muted" style="display:block;margin-top:2px">Phone</label>
                  <input class="miniInput i-phone" value="{phone}" />
                  <label class="small muted" style="display:block;margin-top:8px">Name</label>
                  <input class="miniInput i-name" value="{(name.replace('"','&quot;'))}" />
                </div>
              </div>
            </div>
          </td>

          <td>
            <div class="inlineView">
              {pill}<br><span class="muted">Updated: <span class="v-updated">{updated}</span></span>
            </div>

            <div class="inlineField">
              <label class="small muted" style="display:block;margin-top:2px">Status</label>
              <select class="miniSelect i-status">
                <option value="0" {"selected" if not opted_out else ""}>OPTED IN</option>
                <option value="1" {"selected" if opted_out else ""}>OPTED OUT</option>
              </select>
              <div class="muted small" style="margin-top:8px">Updated: <span class="v-updated-2">{updated}</span></div>
            </div>
          </td>

          <td class="rowActions">
            <div class="inlineView">
              <button type="button" class="btn2" onclick="startEdit({cid})">Edit</button>
              <button type="button" class="btnDanger" onclick="openDeleteModal({cid})">Delete</button>

              <form method="post" action="/admin/contacts/optin" style="margin-left:8px">
                <input type="hidden" name="phone" value="{phone}">
                <button type="submit">Opt In</button>
              </form>

              <form method="post" action="/admin/contacts/optout">
                <input type="hidden" name="phone" value="{phone}">
                <button type="submit" class="btn2">Opt Out</button>
              </form>
            </div>

            <div class="inlineField">
              <button type="button" onclick="saveEdit({cid})">Save</button>
              <button type="button" class="btnGhost" onclick="cancelEdit({cid})">Cancel</button>
            </div>
          </td>
        </tr>
        """

    body = f"""
    <h2>Admin – Contacts</h2>

    <div class="actionsTop">
      <div class="left">
        <form method="get" class="searchRow" style="margin:0">
          <input name="q" placeholder="Search phone or name" value="{q.replace('"', '&quot;')}" />
          <button type="submit">Search</button>
          <a href="/admin/contacts" class="muted" style="align-self:center">Clear</a>
        </form>
      </div>
      <div class="right">
        <a href="{export_link}"><button type="button" class="btn2">Export CSV</button></a>
      </div>
    </div>

    <div class="card">
      <p class="muted">Showing <strong>{len(contacts)}</strong> contact(s){(" (filtered)" if q else "")}.</p>
      <table>
        <tr>
          <th>Contact</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
        {rows_html if rows_html else "<tr><td colspan='3'>No contacts found.</td></tr>"}
      </table>
      <p class="muted small" style="margin-top:12px">Tip: Inline edit updates the DB + exports files immediately.</p>
    </div>

    <!-- Delete Modal -->
    <div id="modalOverlay" class="modalOverlay" role="dialog" aria-modal="true">
      <div class="modal">
        <h3>Delete contact?</h3>
        <div class="muted" id="deleteDesc">This cannot be undone.</div>
        <div class="row">
          <button type="button" class="btnGhost" onclick="closeDeleteModal()">Cancel</button>
          <button type="button" class="btnDanger" onclick="confirmDelete()">Delete</button>
        </div>
      </div>
    </div>

    <script>
      let __deleteId = null;

      function rowEl(id) {{
        return document.getElementById('row-' + id);
      }}

      function startEdit(id) {{
        const r = rowEl(id);
        if(!r) return;
        r.classList.add('editing');
      }}

      function cancelEdit(id) {{
        const r = rowEl(id);
        if(!r) return;
        // reset inputs back to current displayed values
        r.querySelector('.i-phone').value = r.querySelector('.v-phone').textContent.trim();
        r.querySelector('.i-name').value = r.querySelector('.v-name').textContent.trim();
        const pillText = (r.querySelector('.inlineView .pill') || {{textContent:''}}).textContent || '';
        r.querySelector('.i-status').value = pillText.includes('OUT') ? '1' : '0';
        r.classList.remove('editing');
      }}

      async function saveEdit(id) {{
        const r = rowEl(id);
        if(!r) return;

        const phone = (r.querySelector('.i-phone').value || '').trim();
        const name = (r.querySelector('.i-name').value || '').trim();
        const opted_out = (r.querySelector('.i-status').value || '0');

        try {{
          const res = await fetch('/admin/api/contacts/' + id, {{
            method: 'POST',
            headers: {{'Content-Type':'application/json'}},
            credentials: 'same-origin',
            body: JSON.stringify({{phone, name, opted_out}})
          }});
          const data = await res.json();
          if(!data.ok) {{
            showToast(data.error || 'Update failed.');
            return;
          }}

          // Update display fields
          r.querySelector('.v-phone').textContent = data.contact.phone;
          r.querySelector('.v-name').textContent = data.contact.name || '';

          const statusCell = r.children[1];
          const pill = statusCell.querySelector('.inlineView .pill');
          if(pill) {{
            const out = (parseInt(data.contact.opted_out) === 1);
            pill.textContent = out ? 'OPTED OUT' : 'OPTED IN';
            pill.className = 'pill ' + (out ? 'out' : 'in');
          }}

          const upd = data.contact.updated_at || '';
          const vUpdated = r.querySelector('.v-updated');
          const vUpdated2 = r.querySelector('.v-updated-2');
          if(vUpdated) vUpdated.textContent = upd;
          if(vUpdated2) vUpdated2.textContent = upd;

          r.classList.remove('editing');
          showToast('Saved.');
        }} catch(e) {{
          showToast('Update failed.');
        }}
      }}

      function openDeleteModal(id) {{
        const r = rowEl(id);
        if(!r) return;
        __deleteId = id;

        const phone = (r.querySelector('.v-phone')?.textContent || '').trim();
        const name = (r.querySelector('.v-name')?.textContent || '').trim();
        const desc = document.getElementById('deleteDesc');
        desc.textContent = 'Delete ' + (name ? (name + ' ') : '') + '(' + phone + ')? This cannot be undone.';

        const o = document.getElementById('modalOverlay');
        o.style.display = 'flex';
      }}

      function closeDeleteModal() {{
        __deleteId = null;
        const o = document.getElementById('modalOverlay');
        o.style.display = 'none';
      }}

      async function confirmDelete() {{
        if(__deleteId == null) return;
        const id = __deleteId;

        try {{
          const res = await fetch('/admin/api/contacts/' + id + '/delete', {{
            method: 'POST',
            credentials: 'same-origin'
          }});
          const data = await res.json();
          if(!data.ok) {{
            showToast(data.error || 'Delete failed.');
            return;
          }}
          const r = rowEl(id);
          if(r) r.remove();
          closeDeleteModal();
          showToast('Deleted.');
        }} catch(e) {{
          showToast('Delete failed.');
        }}
      }}

      // Close modal by clicking overlay
      document.getElementById('modalOverlay').addEventListener('click', (e)=>{{
        if(e.target && e.target.id === 'modalOverlay') closeDeleteModal();
      }});
    </script>
    """
    return render_admin("Contacts", body)


@app.route("/admin/contacts/export.csv", methods=["GET"])
def admin_contacts_export_csv():
    gate = require_admin()
    if gate:
        return gate

    q = (request.args.get("q") or "").strip()
    contacts = list_contacts(q=q)

    def csv_lines():
        yield "id,phone,name,opted_out,created_at,updated_at\n"
        for c in contacts:
            # basic csv escaping
            def esc(v: Any) -> str:
                s = "" if v is None else str(v)
                s = s.replace('"', '""')
                return f'"{s}"'
            yield ",".join(
                [
                    esc(c.get("id")),
                    esc(c.get("phone")),
                    esc(c.get("name")),
                    esc(c.get("opted_out")),
                    esc(c.get("created_at")),
                    esc(c.get("updated_at")),
                ]
            ) + "\n"

    filename = "contacts_export.csv" if not q else "contacts_export_filtered.csv"
    headers = {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": f'attachment; filename="{filename}"',
    }
    return Response(csv_lines(), headers=headers)


@app.route("/admin/audit", methods=["GET"])
def admin_audit():
    gate = require_admin()
    if gate:
        return gate

    # show latest 250
    init_db()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, actor, action, contact_id, before_json, after_json, ip, created_at
            FROM audit_log
            ORDER BY id DESC
            LIMIT 250
            """
        ).fetchall()

    def summarize(before_json: Optional[str], after_json: Optional[str]) -> str:
        try:
            before = json.loads(before_json) if before_json else None
        except Exception:
            before = None
        try:
            after = json.loads(after_json) if after_json else None
        except Exception:
            after = None

        if after and not before:
            return f"Created {after.get('phone','')}" + (f" ({after.get('name','')})" if (after.get("name") or "").strip() else "")
        if before and not after:
            return f"Deleted {before.get('phone','')}" + (f" ({before.get('name','')})" if (before.get("name") or "").strip() else "")
        if before and after:
            changes = []
            for k in ["phone", "name", "opted_out"]:
                if str(before.get(k)) != str(after.get(k)):
                    changes.append(f"{k}: {before.get(k)} → {after.get(k)}")
            if changes:
                return "; ".join(changes)
            return "Updated (no visible change)"
        return ""

    rows_html = ""
    for r in rows:
        action = (r["action"] or "").upper()
        actor = r["actor"] or ""
        when = r["created_at"] or ""
        ip = r["ip"] or ""
        summary = summarize(r["before_json"], r["after_json"])
        rows_html += f"""
        <tr>
          <td><strong>{when}</strong><br><span class="muted small">{ip}</span></td>
          <td><strong>{actor}</strong></td>
          <td><span class="pill {'out' if 'DELETE' in action or 'OUT' in action else 'in'}">{action}</span></td>
          <td class="muted">{summary}</td>
        </tr>
        """

    body = f"""
    <h2>Admin – Audit Log</h2>
    <div class="card">
      <p class="muted">Latest <strong>{len(rows)}</strong> actions.</p>
      <table>
        <tr>
          <th>Time</th>
          <th>Actor</th>
          <th>Action</th>
          <th>Details</th>
        </tr>
        {rows_html if rows_html else "<tr><td colspan='4'>No audit entries yet.</td></tr>"}
      </table>
      <p class="muted small" style="margin-top:12px">Note: Actions include create/update/delete/opt in/opt out from the admin panel (and some SMS actions as actor <span class="kbd">system</span>).</p>
    </div>
    """
    return render_admin("Audit Log", body)


@app.route("/admin/contacts/optout", methods=["POST"])
def admin_contacts_optout():
    gate = require_admin()
    if gate:
        return gate

    phone = request.form.get("phone") or ""
    phone = normalize_e164(phone, DEFAULT_REGION) or phone
    if phone:
        set_opted_out(phone, 1, actor=current_actor(), log=True)
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


@app.route("/admin/contacts/optin", methods=["POST"])
def admin_contacts_optin():
    gate = require_admin()
    if gate:
        return gate

    phone = request.form.get("phone") or ""
    phone = normalize_e164(phone, DEFAULT_REGION) or phone
    if phone:
        set_opted_out(phone, 0, actor=current_actor(), log=True)
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


# (Kept) Old delete endpoint for safety/backward compatibility
@app.route("/admin/contacts/delete", methods=["POST"])
def admin_contacts_delete():
    gate = require_admin()
    if gate:
        return gate

    phone = request.form.get("phone") or ""
    phone = normalize_e164(phone, DEFAULT_REGION) or phone
    if phone:
        delete_contact(phone, actor=current_actor(), log=True)
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


# (Kept) Old edit page (optional fallback)
@app.route("/admin/contacts/edit", methods=["GET", "POST"])
def admin_contacts_edit():
    gate = require_admin()
    if gate:
        return gate

    if request.method == "GET":
        phone = request.args.get("phone", "") or ""
        phone_norm = normalize_e164(phone, DEFAULT_REGION) or phone
        c = get_contact_by_phone(phone_norm)
        if not c:
            return redirect(url_for("admin_contacts"))

        body = f"""
        <h2>Edit Contact (Fallback)</h2>
        <div class="card" style="max-width:720px">
          <p class="muted small">This page is kept as a backup. The main workflow is inline editing on the Contacts page.</p>
          <form method="post">
            <input type="hidden" name="old_phone" value="{c['phone']}">

            <label>Phone number</label>
            <input name="new_phone" value="{c['phone']}" required />

            <label>Name</label>
            <input name="name" value="{(c['name'] or '').replace('"','&quot;')}" />

            <label>Status</label>
            <select name="opted_out">
              <option value="0" {"selected" if int(c["opted_out"])==0 else ""}>OPTED IN</option>
              <option value="1" {"selected" if int(c["opted_out"])==1 else ""}>OPTED OUT</option>
            </select>

            <div style="margin-top:14px">
              <button type="submit">Save</button>
              <a href="/admin/contacts" class="muted" style="margin-left:12px">Cancel</a>
            </div>
          </form>
        </div>
        """
        return render_admin("Edit Contact", body)

    # POST
    old_phone = request.form.get("old_phone") or ""
    new_phone_raw = request.form.get("new_phone") or ""
    new_name_raw = request.form.get("name") or ""
    opted_out_raw = request.form.get("opted_out") or "0"

    old_phone = normalize_e164(old_phone, DEFAULT_REGION) or old_phone
    new_phone = normalize_e164(new_phone_raw, DEFAULT_REGION)
    if not new_phone:
        return render_admin("Edit Contact", "<p class='err'>Invalid phone number.</p><p><a href='/admin/contacts'>Back</a></p>")

    new_name = clean_name(new_name_raw)

    # audit here too
    before = get_contact_by_phone(old_phone)
    err = update_contact(old_phone, new_phone, new_name)
    if err:
        return render_admin("Edit Contact", f"<p class='err'>{err}</p><p><a href='/admin/contacts'>Back</a></p>")

    try:
        opted_out_val = 1 if str(opted_out_raw).strip() == "1" else 0
        set_opted_out(new_phone, opted_out_val, actor=current_actor(), log=False)  # we'll log once as "update"
    except Exception:
        pass

    after = get_contact_by_phone(new_phone)
    if after:
        audit_log(current_actor(), "update", after.get("id"), before, after)

    export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


# -------------------------
# Twilio Webhook
# -------------------------
@app.route("/sms", methods=["POST"])
def inbound_sms():
    init_db()

    from_number = request.form.get("From", "")
    body = (request.form.get("Body") or "").strip()
    toks = tokens_upper(body)

    phone = normalize_e164(from_number, DEFAULT_REGION)

    resp = MessagingResponse()
    if not phone:
        resp.message("Invalid number. Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    c = get_contact_by_phone(phone)

    if toks & OPTOUT_KEYWORDS:
        if c:
            set_opted_out(phone, 1, actor="system", log=True)
        else:
            add_contact(phone, "", actor="system", log=True)
            set_opted_out(phone, 1, actor="system", log=True)
        export_contacts_csv_and_optouts()
        resp.message("You’re opted out. Reply START to resubscribe.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if toks & HELP_KEYWORDS:
        resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if toks & OPTIN_KEYWORDS:
        if c:
            set_opted_out(phone, 0, actor="system", log=True)
            export_contacts_csv_and_optouts()
            resp.message("You’re subscribed! Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

        # new subscriber
        if ASK_NAME_ON_JOIN:
            add_contact(phone, "", actor="system", log=True)
            set_opted_out(phone, 0, actor="system", log=True)
            export_contacts_csv_and_optouts()
            resp.message("You’re subscribed! Reply with your first name (example: Joey). Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}
        else:
            add_contact(phone, "", actor="system", log=True)
            set_opted_out(phone, 0, actor="system", log=True)
            export_contacts_csv_and_optouts()
            resp.message("You’re subscribed! Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

    # If opted in and name is empty, treat message as name (only if not a keyword)
    if c and int(c["opted_out"]) == 0 and ASK_NAME_ON_JOIN and not (c["name"] or "").strip():
        name = clean_name(body)
        if name:
            before = get_contact_by_phone(phone)
            update_contact(phone, phone, name)
            after = get_contact_by_phone(phone)
            if after:
                audit_log("system", "update", after.get("id"), before, after)
            export_contacts_csv_and_optouts()
            resp.message(f"Thanks, {name}! You’re all set. Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

    resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
    return str(resp), 200, {"Content-Type": "application/xml"}
