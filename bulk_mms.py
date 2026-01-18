import csv
import os
import time
from typing import Dict, List, Set, Optional

from dotenv import load_dotenv
from twilio.rest import Client
import phonenumbers

# =========================
# CONFIG
# =========================
CONTACTS_CSV = "contacts.csv"
LOG_FILE = "send_log.csv"
OPTOUT_FILE = "optouts.txt"        # optional; script will skip numbers listed here (one per line, E.164)

DEFAULT_REGION = "US"
SECONDS_BETWEEN_MESSAGES = 0.40    # throttle
DRY_RUN = False                   # set True to test without sending

# Your message + media
MESSAGE_TEMPLATE = (
    "J Maslanka Estate Sales: Hi {name} — New sale this weekend. "
    "Reply STOP to opt out."
)

# IMPORTANT: must be a publicly accessible *direct* image URL
IMAGE_URL = "https://i.imgur.com/xlpj9Ve.png"

# =========================


def load_optouts(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def normalize_e164(raw: str, default_region: str = DEFAULT_REGION) -> Optional[str]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        if raw.startswith("+"):
            parsed = phonenumbers.parse(raw, None)
        else:
            parsed = phonenumbers.parse(raw, default_region)

        if not phonenumbers.is_valid_number(parsed):
            return None

        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None


def load_contacts(csv_path: str) -> List[Dict[str, str]]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing {csv_path}. Create it with columns: phone,name")

    contacts: List[Dict[str, str]] = []
    seen: Set[str] = set()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "phone" not in reader.fieldnames:
            raise ValueError("contacts.csv must have a header row including at least: phone (and optionally name).")

        for row in reader:
            phone_raw = row.get("phone", "")
            name = (row.get("name") or "").strip()

            phone = normalize_e164(phone_raw)
            if not phone:
                continue

            if phone in seen:
                continue
            seen.add(phone)

            contacts.append({"phone": phone, "name": name})

    return contacts


def append_log(phone: str, name: str, status: str, sid_or_error: str) -> None:
    file_exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["phone", "name", "status", "sid_or_error"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {"phone": phone, "name": name, "status": status, "sid_or_error": sid_or_error}
        )


def ensure_env() -> Dict[str, str]:
    load_dotenv()
    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
    auth_token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
    messaging_service_sid = os.getenv("TWILIO_MESSAGING_SERVICE_SID", "").strip()  # MG...

    missing = [k for k, v in {
        "TWILIO_ACCOUNT_SID": account_sid,
        "TWILIO_AUTH_TOKEN": auth_token,
        "TWILIO_MESSAGING_SERVICE_SID": messaging_service_sid
    }.items() if not v]

    if missing:
        raise RuntimeError(
            f"Missing env vars in .env: {', '.join(missing)}\n"
            f"NOTE: For A2P 10DLC you must send via Messaging Service SID (MG...)."
        )

    return {
        "account_sid": account_sid,
        "auth_token": auth_token,
        "messaging_service_sid": messaging_service_sid
    }


def build_body(template: str, name: str) -> str:
    safe_name = name if name else "there"
    return template.replace("{name}", safe_name)


def send_bulk_mms() -> None:
    creds = ensure_env()
    client = Client(creds["account_sid"], creds["auth_token"])

    optouts = load_optouts(OPTOUT_FILE)
    contacts = load_contacts(CONTACTS_CSV)

    print(f"Contacts loaded: {len(contacts)}")
    print(f"Opt-outs loaded: {len(optouts)}")
    print(f"DRY_RUN: {DRY_RUN}")
    print("Starting send...\n")

    sent = failed = skipped = 0

    for i, c in enumerate(contacts, start=1):
        to_number = c["phone"]
        name = c["name"]

        if to_number in optouts:
            skipped += 1
            append_log(to_number, name, "SKIPPED_OPTOUT", "")
            print(f"[{i}/{len(contacts)}] SKIP (opt-out) -> {to_number}")
            continue

        body = build_body(MESSAGE_TEMPLATE, name)

        if DRY_RUN:
            sent += 1
            append_log(to_number, name, "DRY_RUN", "")
            print(f"[{i}/{len(contacts)}] DRY_RUN -> {to_number}")
        else:
            try:
                # IMPORTANT: For A2P 10DLC you must send via messaging_service_sid (MG...)
                msg = client.messages.create(
                    messaging_service_sid=creds["messaging_service_sid"],
                    to=to_number,
                    body=body,
                    #media_url=[IMAGE_URL],  # comment out to test SMS-only
                )
                sent += 1
                append_log(to_number, name, "SENT", msg.sid)
                print(f"[{i}/{len(contacts)}] SENT -> {to_number} (SID: {msg.sid})")
            except Exception as e:
                failed += 1
                append_log(to_number, name, "FAILED", str(e))
                print(f"[{i}/{len(contacts)}] FAILED -> {to_number}: {e}")

        time.sleep(SECONDS_BETWEEN_MESSAGES)

    print("\nDone.")
    print(f"Sent/Dry: {sent} | Failed: {failed} | Skipped: {skipped}")
    print(f"Log saved to: {LOG_FILE}")


if __name__ == "__main__":
    send_bulk_mms()
