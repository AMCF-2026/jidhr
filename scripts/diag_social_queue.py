"""
Jidhr diagnostic: social-queue probe.

Two modes:

  MODE 1 — READ PROBE (default, no args, GET-only)
    Prints publishable channels, WAITING-broadcast count + per-channel
    breakdown, one full sample broadcast JSON, a compact line per
    waiting item, and a FINDINGS footer naming pagination mechanism,
    timestamp field/units, and where message/link/photo live.

  MODE 2 — WRITE PROBE  (--write-probe --channel-guid <GUID>)
    Creates ONE real, far-future scheduled broadcast (2027-01-15 10:00
    UTC) carrying a "DO NOT PUBLISH" body, then attempts to delete it
    via the legacy /broadcast/v1/broadcasts/{id} endpoint. Prints raw
    HTTP status + body for every call. If DELETE fails, prints a loud
    manual-cleanup notice with the broadcast id.

Run from Railway shell:
    /opt/venv/bin/python scripts/diag_social_queue.py
    /opt/venv/bin/python scripts/diag_social_queue.py --write-probe --channel-guid <GUID>

Auth is reused from clients.hubspot.HubSpotClient (no token duplication).
"""

import argparse
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from urllib.parse import urlparse

# Make the repo root importable when run as `python scripts/...`.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

from clients.hubspot import HubSpotClient


PAGE_SIZE = 100
PROBE_TRIGGER_AT_UTC = datetime(2027, 1, 15, 10, 0, tzinfo=timezone.utc)
PROBE_MESSAGE = "JIDHR DIAGNOSTIC — DO NOT PUBLISH — safe to delete"
SEP = "=" * 70


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ms_from_utc(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _humanize_ts(value):
    """Decode value as epoch ms or epoch s by magnitude. Returns (unit, utc_str)."""
    if not isinstance(value, (int, float)):
        return ("not-numeric", repr(value))
    try:
        if value >= 10**12:
            return ("epoch-ms",
                    datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                    .strftime("%Y-%m-%d %H:%M:%S UTC"))
        return ("epoch-s",
                datetime.fromtimestamp(value, tz=timezone.utc)
                .strftime("%Y-%m-%d %H:%M:%S UTC"))
    except (ValueError, OSError, OverflowError):
        return ("decode-error", repr(value))


def _short(s, n=60):
    if not isinstance(s, str):
        return ""
    s = s.replace("\n", " ").replace("\r", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _dump_json(body):
    try:
        print(json.dumps(body, indent=2, default=str))
    except Exception as e:
        print(f"(could not pretty-print: {e})")
        print(body)


# Drive-share rewrite + image-extension sanity check, both used by the
# write probe when --photo-url is supplied.

_DRIVE_FILE_RE = re.compile(r"^/file/d/([^/]+)")
_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".webp")


def _rewrite_drive_url(url):
    """If drive.google.com /file/d/<ID>/..., rewrite to direct-download.
    Returns (final_url, was_rewritten)."""
    try:
        parsed = urlparse(url)
    except Exception:
        return (url, False)
    if (parsed.netloc or "").lower() != "drive.google.com":
        return (url, False)
    m = _DRIVE_FILE_RE.match(parsed.path or "")
    if not m:
        return (url, False)
    return (
        f"https://drive.google.com/uc?export=download&id={m.group(1)}",
        True,
    )


def _looks_like_image_url(url):
    try:
        path = (urlparse(url).path or "").lower()
    except Exception:
        return False
    return path.endswith(_IMAGE_EXTS)


def _print_media_fields(body):
    """Print broadcastMediaType, content.photoUrl, content.link, and
    extraData.files (count + first file's url/mediaType/fileStatus)
    on their own labeled lines."""
    if not isinstance(body, dict):
        print("  (no parseable body for media-field readout)")
        return
    print(f"  broadcastMediaType: {body.get('broadcastMediaType')!r}")
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    print(f"  content.photoUrl:   {content.get('photoUrl')!r}")
    print(f"  content.link:       {content.get('link')!r}")
    extra = body.get("extraData") if isinstance(body.get("extraData"), dict) else {}
    files = extra.get("files") if isinstance(extra.get("files"), list) else []
    print(f"  extraData.files:    count={len(files)}")
    if files and isinstance(files[0], dict):
        f0 = files[0]
        print(f"    [0].url:        {f0.get('url')!r}")
        print(f"    [0].mediaType:  {f0.get('mediaType')!r}")
        print(f"    [0].fileStatus: {f0.get('fileStatus')!r}")


# ---------------------------------------------------------------------------
# MODE 1 — READ PROBE
# ---------------------------------------------------------------------------

def _get_broadcasts_page(hubspot, limit, offset, status=None):
    url = f"{hubspot.base_url}/broadcast/v1/broadcasts"
    params = {"limit": limit}
    if offset:
        params["offset"] = offset
    if status:
        params["status"] = status
    return requests.get(url, headers=hubspot.headers, params=params, timeout=30)


def read_probe(hubspot):
    # --- Channels ---
    print(SEP); print("CHANNELS"); print(SEP)
    url = f"{hubspot.base_url}/broadcast/v1/channels/setting/publish/current"
    chans_resp = requests.get(url, headers=hubspot.headers, timeout=30)
    if chans_resp.status_code >= 400:
        print(f"channels fetch failed: HTTP {chans_resp.status_code}")
        print(chans_resp.text); return

    channels = chans_resp.json()
    if not isinstance(channels, list):
        print("Unexpected channels response shape:")
        _dump_json(channels); return

    print(f"{'channelGuid':<40} {'type':<25} name")
    print("-" * 90)
    for ch in channels:
        guid = str(ch.get("channelGuid") or ch.get("channel") or "?")
        ctype = str(ch.get("channelType") or "?")
        name = str(ch.get("accountName") or ch.get("pageName") or ch.get("name") or "?")
        print(f"{guid:<40} {ctype:<25} {name}")

    # --- Try server-side WAITING filter, fall back to client-side ---
    print()
    print(SEP); print("BROADCASTS"); print(SEP)
    initial = _get_broadcasts_page(hubspot, PAGE_SIZE, 0, status="WAITING")
    status_param_works = False
    if initial.status_code >= 400:
        print(f"[probe] status=WAITING rejected (HTTP {initial.status_code}); "
              "fetching unfiltered and filtering client-side")
    else:
        try:
            body = initial.json()
        except Exception:
            body = None
        if isinstance(body, list) and body:
            non_waiting = [b for b in body if isinstance(b, dict)
                           and b.get("status") != "WAITING"]
            if not non_waiting:
                status_param_works = True
                print("[probe] status=WAITING appears to filter server-side")
            else:
                print(f"[probe] status=WAITING ignored "
                      f"({len(non_waiting)}/{len(body)} non-waiting on page 1); "
                      "filtering client-side")
        else:
            print("[probe] status=WAITING returned empty/unexpected; "
                  "fetching unfiltered for visibility")

    # --- Page through all (one filter mode or the other) ---
    all_b = []
    offset = 0
    pages = 0
    MAX_PAGES = 100
    while pages < MAX_PAGES:
        resp = _get_broadcasts_page(
            hubspot, PAGE_SIZE, offset,
            status="WAITING" if status_param_works else None,
        )
        pages += 1
        if resp.status_code >= 400:
            print(f"[probe] page {pages} HTTP {resp.status_code}: {resp.text[:200]}")
            break
        try:
            page = resp.json()
        except Exception as e:
            print(f"[probe] page {pages} JSON decode failed: {e}"); break
        if not isinstance(page, list) or not page:
            break
        all_b.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    print(f"\nFetched {len(all_b)} broadcasts across {pages} page(s)")

    # --- Status histogram (across ALL fetched) ---
    by_status = {}
    for b in all_b:
        if isinstance(b, dict):
            by_status.setdefault(b.get("status") or "(none)", []).append(b)

    print("\nStatus counts (all fetched):")
    for st, items in sorted(by_status.items()):
        print(f"  {st}: {len(items)}")

    waiting = by_status.get("WAITING", [])
    print(f"\nWAITING total: {len(waiting)}")

    # --- Per-channel breakdown for WAITING ---
    if waiting:
        per_ch = {}
        for b in waiting:
            ck = b.get("channelKey") or "?"
            prefix = ck.split(":", 1)[0] if ":" in ck else ck
            per_ch[prefix] = per_ch.get(prefix, 0) + 1
        print("WAITING per channel:")
        for ch, n in sorted(per_ch.items(), key=lambda kv: -kv[1]):
            print(f"  {ch}: {n}")

    # --- One full sample ---
    if waiting:
        print(); print(SEP); print("SAMPLE WAITING BROADCAST (full JSON)"); print(SEP)
        _dump_json(waiting[0])

    # --- Compact line per waiting ---
    if waiting:
        print(); print(SEP); print(f"COMPACT WAITING LIST ({len(waiting)})"); print(SEP)
        for b in waiting:
            ck = (b.get("channelKey") or "?")[:30]
            guid = (b.get("broadcastGuid") or "?")[:36]
            trig = b.get("triggerAt")
            unit, human = _humanize_ts(trig)
            link = b.get("messageUrl") or ""
            photo = "y" if (b.get("photoUrl") or b.get("photo")) else "n"
            msg = _short(b.get("messageText") or "", 60)
            print(f"- ch={ck:<30} guid={guid:<36} "
                  f"trigger_raw={trig} ({unit}={human}) "
                  f"link={(link or '-')[:40]} photo={photo}")
            print(f"  msg: {msg}")

    # --- Findings ---
    print(); print(SEP); print("FINDINGS"); print(SEP)
    print(f"Pagination:           limit + offset, page size {PAGE_SIZE} "
          "(matches V4.5 client)")
    print(f"status=WAITING param: "
          f"{'works server-side' if status_param_works else 'ignored — used client-side filter'}")
    if waiting:
        sample = waiting[0]
        for key in ("triggerAt", "createdAt", "finishedAt", "scheduledAt", "publishAt"):
            if key in sample:
                unit, human = _humanize_ts(sample[key])
                print(f"  {key}: {sample[key]} ({unit} → {human})")
        print(f"messageText present: {'messageText' in sample}")
        print(f"messageUrl  present: {'messageUrl' in sample}  (link)")
        print(f"photoUrl    present: {'photoUrl' in sample}   "
              f"photo present: {'photo' in sample}")


# ---------------------------------------------------------------------------
# MODE 2 — WRITE PROBE
# ---------------------------------------------------------------------------

def write_probe(hubspot, channel_guid, photo_url=None, link=None):
    print(SEP); print("WRITE PROBE"); print(SEP)
    print(f"channelKey:           {channel_guid}")
    print(f"triggerAt UTC:        {PROBE_TRIGGER_AT_UTC.isoformat()}")
    print(f"triggerAt epoch-ms:   {_ms_from_utc(PROBE_TRIGGER_AT_UTC)}")

    final_photo_url = None
    if photo_url:
        rewritten, was_rewritten = _rewrite_drive_url(photo_url)
        if was_rewritten:
            print(f"photoUrl original:    {photo_url}")
            print(f"photoUrl rewritten:   {rewritten}")
            print( "                      (drive.google.com /file/d/ → direct-download)")
        else:
            print(f"photoUrl:             {photo_url}")
        final_photo_url = rewritten
        if not _looks_like_image_url(final_photo_url):
            print()
            print("⚠️  WARNING: photoUrl does not end in .jpg/.jpeg/.png/.gif/.webp")
            print("    HubSpot may reject non-image media submitted as PHOTO")
            print("    (known failure: .mp4 → ERROR_ARCHIVE). Sending anyway —")
            print("    collecting the rejection is valid probe data.")

    if link:
        print(f"link:                 {link}")
    print()

    content_block = {"body": PROBE_MESSAGE}
    if final_photo_url:
        content_block["photoUrl"] = final_photo_url
    if link:
        content_block["link"] = link

    payload = {
        "channelGuid": channel_guid,
        "triggerAt": _ms_from_utc(PROBE_TRIGGER_AT_UTC),
        "content": content_block,
    }
    create_url = f"{hubspot.base_url}/broadcast/v1/broadcasts"
    print(f"POST {create_url}")
    print("payload:"); _dump_json(payload)

    create_resp = requests.post(
        create_url, headers=hubspot.headers, json=payload, timeout=30
    )
    print(f"\n→ status: {create_resp.status_code}")
    print("→ body:")
    try:
        create_body = create_resp.json()
        _dump_json(create_body)
        print("→ media fields:")
        _print_media_fields(create_body)
    except Exception:
        print(create_resp.text or "(empty)")

    if create_resp.status_code >= 400:
        print("\nCreate failed; nothing to clean up.")
        return

    try:
        broadcast = create_resp.json()
    except Exception as e:
        print(f"\nCould not parse creation response JSON: {e}"); return

    bid = (broadcast.get("broadcastGuid")
           or broadcast.get("broadcastId")
           or broadcast.get("id"))
    if not bid:
        print("\nNo broadcast id in response; cannot verify or clean up.")
        return

    print(f"\nCreated broadcast id: {bid}")

    detail_url = f"{hubspot.base_url}/broadcast/v1/broadcasts/{bid}"
    print(f"\nGET {detail_url}")
    detail_resp = requests.get(detail_url, headers=hubspot.headers, timeout=30)
    print(f"→ status: {detail_resp.status_code}")
    try:
        detail = detail_resp.json()
        print(f"→ status field: {detail.get('status')}")
        _dump_json(detail)
        print("→ media fields:")
        _print_media_fields(detail)
    except Exception:
        print(detail_resp.text or "(empty)")

    print(f"\nDELETE {detail_url}")
    del_resp = requests.delete(detail_url, headers=hubspot.headers, timeout=30)
    print(f"→ status: {del_resp.status_code}")
    print("→ body:")
    try: _dump_json(del_resp.json())
    except Exception: print(del_resp.text or "(empty)")

    if del_resp.status_code >= 400:
        print()
        print("!" * 70)
        print("DELETE FAILED — MANUAL CLEANUP REQUIRED")
        print("!" * 70)
        print(f"  broadcast id: {bid}")
        print(f"  channelKey:   {channel_guid}")
        print(f"  triggerAt:    {PROBE_TRIGGER_AT_UTC.isoformat()} UTC")
        print()
        print("  Open HubSpot UI → Marketing → Social → find the broadcast above,")
        print("  and delete it manually BEFORE its triggerAt fires.")
        print("!" * 70)
        return

    print(f"\nGET {detail_url}   (verify cancellation)")
    verify_resp = requests.get(detail_url, headers=hubspot.headers, timeout=30)
    print(f"→ status: {verify_resp.status_code}")
    try:
        verify = verify_resp.json()
        print(f"→ status field: {verify.get('status')}")
        _dump_json(verify)
    except Exception:
        print(verify_resp.text or "(empty)")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Social-queue diagnostic probe")
    p.add_argument("--write-probe", action="store_true",
                   help="Create + cancel one far-future test broadcast.")
    p.add_argument("--channel-guid",
                   help="Required with --write-probe. The channelKey / channelGuid.")
    p.add_argument("--photo-url",
                   help="Write-probe only: add photoUrl to payload content. "
                        "drive.google.com /file/d/<ID>/... is rewritten to "
                        "the uc?export=download form before sending.")
    p.add_argument("--link",
                   help="Write-probe only: add link to payload content.")
    args = p.parse_args()

    hubspot = HubSpotClient()
    if not hubspot.access_token:
        print("ERROR: HUBSPOT_ACCESS_TOKEN is not set in this environment.")
        return 2

    if args.write_probe or args.channel_guid:
        if not (args.write_probe and args.channel_guid):
            print("ERROR: --write-probe and --channel-guid must be used together.")
            print("       Run with no args for the read-only probe.")
            return 2
        try:
            write_probe(
                hubspot,
                args.channel_guid,
                photo_url=args.photo_url,
                link=args.link,
            )
        except Exception:
            print("\nWRITE PROBE RAISED:")
            traceback.print_exc()
            return 1
        return 0

    read_probe(hubspot)
    return 0


if __name__ == "__main__":
    sys.exit(main())
