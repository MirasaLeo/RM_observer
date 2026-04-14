#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Serve index.html locally and persist school notes to CSV."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import csv
import hashlib
import json
import re
import time
from datetime import datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse
from urllib import error as urllib_error
from urllib import request as urllib_request

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "school_notes.csv"
LOGO_CACHE_DIR = DATA_DIR / "logo_cache"
NOTE_IMAGE_DIR = DATA_DIR / "imgs"
CSV_FIELDS = [
    "school_key",
    "college_name",
    "team_name",
    "zone_names",
    "slot_id",
    "slot_label",
    "note",
    "image_paths",
    "updated_at",
]
DEFAULT_LOGO_URL = "https://rm-static.djicdn.com/robomasters/public/school.png"
LOGO_CACHE_TTL_S = 12 * 60 * 60
MAX_IMAGE_BYTES = 8 * 1024 * 1024
FALLBACK_LOGO_BYTES = b"""<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96"><rect width="96" height="96" rx="48" fill="#0f2232"/><circle cx="48" cy="36" r="16" fill="#2fc0ff" fill-opacity="0.22"/><path d="M28 70c4-12 13-18 20-18s16 6 20 18" fill="none" stroke="#cde9ff" stroke-width="6" stroke-linecap="round"/></svg>"""
UPSTREAM_ENDPOINTS = {
    "schedule": "https://rm-static.djicdn.com/live_json/schedule.json",
    "ranking": "https://rm-static.djicdn.com/live_json/group_rank_info.json",
    "robot": "https://rm-static.djicdn.com/live_json/robot_data.json",
}
UPSTREAM_HEADERS = {
    "Accept": "application/json,text/plain,*/*",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.robomaster.com/",
}


def ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    NOTE_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            writer.writeheader()


def csv_path_label() -> str:
    return CSV_PATH.relative_to(ROOT).as_posix()


def load_rows() -> list[dict[str, str]]:
    ensure_storage()
    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [
            {field: str(row.get(field, "")) for field in CSV_FIELDS}
            for row in reader
        ]


def write_rows(rows: list[dict[str, str]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row.get("college_name", ""),
            row.get("team_name", ""),
            row.get("slot_id", ""),
        ),
    )
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def build_notes_map(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    notes: dict[str, dict[str, str]] = {}
    for row in rows:
        school_key = row.get("school_key", "").strip()
        slot_id = row.get("slot_id", "").strip()
        note = row.get("note", "")
        if not school_key or not slot_id:
            continue
        notes.setdefault(school_key, {})[slot_id] = note
    return notes


def build_images_map(rows: list[dict[str, str]]) -> dict[str, dict[str, list[str]]]:
    images: dict[str, dict[str, list[str]]] = {}
    for row in rows:
        school_key = row.get("school_key", "").strip()
        slot_id = row.get("slot_id", "").strip()
        if not school_key or not slot_id:
            continue
        image_paths = parse_image_paths(row.get("image_paths", ""))
        if not image_paths:
            continue
        images.setdefault(school_key, {})[slot_id] = image_paths
    return images


def parse_image_paths(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except json.JSONDecodeError:
        pass
    return [part.strip() for part in text.split(";") if part.strip()]


def normalize_image_payload(images: object) -> dict[str, list[str]]:
    if not isinstance(images, dict):
        raise ValueError("images must be an object")
    normalized: dict[str, list[str]] = {}
    for slot_id, image_paths in images.items():
        slot_id_text = str(slot_id).strip()
        if not slot_id_text:
            continue
        if not isinstance(image_paths, list):
            raise ValueError("each images entry must be an array")
        clean_paths = []
        for image_path in image_paths:
            text = str(image_path).strip()
            if not text:
                continue
            clean_paths.append(text)
        normalized[slot_id_text] = clean_paths
    return normalized


def sanitize_segment(value: str, fallback: str = "item") -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(value).strip().lower()).strip("-")
    return cleaned or fallback


def decode_data_url(data_url: str) -> tuple[str, bytes]:
    matched = re.match(r"^data:(image/[a-zA-Z0-9.+-]+);base64,(.+)$", data_url, re.DOTALL)
    if not matched:
        raise ValueError("invalid image data")
    content_type = matched.group(1)
    try:
        payload = base64.b64decode(matched.group(2), validate=True)
    except ValueError as exc:
        raise ValueError("invalid base64 image payload") from exc
    if not payload:
        raise ValueError("empty image payload")
    if len(payload) > MAX_IMAGE_BYTES:
        raise ValueError("image is too large")
    return content_type, payload


def image_extension(content_type: str, filename: str) -> str:
    by_type = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/svg+xml": ".svg",
        "image/heic": ".heic",
        "image/heif": ".heif",
        "image/bmp": ".bmp",
    }
    if content_type in by_type:
        return by_type[content_type]
    suffix = Path(str(filename or "").strip()).suffix.lower()
    if re.fullmatch(r"\.[a-z0-9]{1,6}", suffix):
        return suffix
    raise ValueError("unsupported image type")


def save_note_image(school_key: str, slot_id: str, filename: str, data_url: str) -> str:
    ensure_storage()
    content_type, payload = decode_data_url(data_url)
    school_dir = NOTE_IMAGE_DIR / sanitize_segment(school_key, "school")
    school_dir.mkdir(parents=True, exist_ok=True)
    ext = image_extension(content_type, filename)
    digest = hashlib.sha1(payload).hexdigest()[:12]
    basename = f"{sanitize_segment(slot_id, 'slot')}-{int(time.time() * 1000)}-{digest}{ext}"
    destination = school_dir / basename
    destination.write_bytes(payload)
    return destination.relative_to(ROOT).as_posix()


def fetch_upstream_json(name: str, url: str) -> dict:
    req = urllib_request.Request(url, headers=UPSTREAM_HEADERS)
    try:
        with urllib_request.urlopen(req, timeout=20) as resp:
            status = getattr(resp, "status", HTTPStatus.OK)
            if status != HTTPStatus.OK:
                raise RuntimeError(f"{name}: {status}")
            payload = resp.read()
    except urllib_error.HTTPError as exc:
        raise RuntimeError(f"{name}: {exc.code} {exc.reason}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"{name}: {exc.reason}") from exc

    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{name}: invalid JSON ({exc.msg})") from exc


def fetch_live_bundle() -> dict:
    results: dict[str, dict] = {}
    errors: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(UPSTREAM_ENDPOINTS)) as executor:
        future_map = {
            executor.submit(fetch_upstream_json, name, url): name
            for name, url in UPSTREAM_ENDPOINTS.items()
        }
        for future, name in future_map.items():
            try:
                results[name] = future.result()
            except Exception as exc:
                errors.append(str(exc))

    if errors:
        raise RuntimeError("; ".join(errors))

    return {
        "schedule": results["schedule"],
        "ranking": results["ranking"],
        "robot": results["robot"],
        "fetchedAt": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def normalize_logo_url(url: str | None) -> str:
    raw = str(url or "").strip()
    if not raw:
        return DEFAULT_LOGO_URL
    if raw.startswith("//"):
        return "https:" + raw

    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return raw
    if raw.startswith("/"):
        return urljoin(DEFAULT_LOGO_URL, raw)
    return DEFAULT_LOGO_URL


def logo_cache_paths(url: str) -> tuple[Path, Path]:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return (
        LOGO_CACHE_DIR / f"{digest}.bin",
        LOGO_CACHE_DIR / f"{digest}.json",
    )


def load_cached_logo(url: str) -> tuple[bytes, str, float] | None:
    data_path, meta_path = logo_cache_paths(url)
    if not data_path.exists() or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        payload = data_path.read_bytes()
        cached_at = float(meta.get("cached_at", meta_path.stat().st_mtime))
        return payload, str(meta.get("content_type", "application/octet-stream")), cached_at
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def save_logo_cache(url: str, payload: bytes, content_type: str) -> None:
    data_path, meta_path = logo_cache_paths(url)
    data_path.write_bytes(payload)
    meta_path.write_text(
        json.dumps(
            {
                "url": url,
                "content_type": content_type,
                "cached_at": time.time(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def fetch_binary(url: str) -> tuple[bytes, str]:
    req = urllib_request.Request(url, headers=UPSTREAM_HEADERS)
    try:
        with urllib_request.urlopen(req, timeout=20) as resp:
            status = getattr(resp, "status", HTTPStatus.OK)
            if status != HTTPStatus.OK:
                raise RuntimeError(f"{status}")
            payload = resp.read()
            content_type = resp.headers.get_content_type() or "application/octet-stream"
            return payload, content_type
    except urllib_error.HTTPError as exc:
        raise RuntimeError(f"{exc.code} {exc.reason}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc


def get_logo_payload(url: str | None) -> tuple[bytes, str]:
    normalized_url = normalize_logo_url(url)
    cached = load_cached_logo(normalized_url)
    stale_cache = cached
    if cached and (time.time() - cached[2]) <= LOGO_CACHE_TTL_S:
        return cached[0], cached[1]

    try:
        payload, content_type = fetch_binary(normalized_url)
        save_logo_cache(normalized_url, payload, content_type)
        return payload, content_type
    except Exception:
        if stale_cache:
            return stale_cache[0], stale_cache[1]
        if normalized_url != DEFAULT_LOGO_URL:
            return get_logo_payload(DEFAULT_LOGO_URL)
        return FALLBACK_LOGO_BYTES, "image/svg+xml"


class RMObserverHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self) -> None:
        path = urlparse(self.path).path
        if path in {
            "/",
            "/index.html",
            "/api/school-notes",
            "/api/live-data",
            "/api/logo",
            "/api/upload-note-image",
            "/favicon.ico",
        } or path.endswith(".csv"):
            self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/api/live-data":
            self.handle_get_live_data()
            return
        if path == "/api/logo":
            self.handle_get_logo(parsed.query)
            return
        if path == "/api/school-notes":
            self.handle_get_school_notes()
            return
        if path == "/favicon.ico":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path == "/":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/upload-note-image":
            self.handle_post_note_image()
            return
        if path != "/api/school-notes":
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API path")
            return
        self.handle_post_school_notes()

    def handle_get_school_notes(self) -> None:
        rows = load_rows()
        self.send_json(
            HTTPStatus.OK,
            {
                "notes": build_notes_map(rows),
                "images": build_images_map(rows),
                "csvPath": csv_path_label(),
                "rowCount": len(rows),
            },
        )

    def handle_get_live_data(self) -> None:
        try:
            payload = fetch_live_bundle()
        except Exception as exc:
            self.send_json(
                HTTPStatus.BAD_GATEWAY,
                {
                    "error": str(exc),
                },
            )
            return

        self.send_json(HTTPStatus.OK, payload)

    def handle_get_logo(self, query: str) -> None:
        logo_url = parse_qs(query).get("url", [""])[0]
        payload, content_type = get_logo_payload(logo_url)
        self.send_binary(
            HTTPStatus.OK,
            payload,
            content_type,
            cache_control=f"public, max-age={LOGO_CACHE_TTL_S}",
        )

    def handle_post_school_notes(self) -> None:
        try:
            payload = self.read_json_body()
        except ValueError as exc:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        school_key = str(payload.get("schoolKey", "")).strip()
        notes = payload.get("notes")
        images = payload.get("images") or {}
        slot_labels = payload.get("slotLabels") or {}
        college_name = str(payload.get("collegeName", "")).strip()
        team_name = str(payload.get("teamName", "")).strip()
        zone_names = payload.get("zoneNames") or []

        if not school_key:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "schoolKey is required"})
            return
        if not isinstance(notes, dict):
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "notes must be an object"})
            return
        try:
            images_map = normalize_image_payload(images)
        except ValueError as exc:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        if not isinstance(slot_labels, dict):
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "slotLabels must be an object"})
            return

        zone_name_text = " / ".join(
            str(item).strip() for item in zone_names if str(item).strip()
        )
        updated_at = datetime.now().astimezone().isoformat(timespec="seconds")

        rows = [row for row in load_rows() if row.get("school_key", "") != school_key]
        saved_count = 0
        slot_ids = set(images_map.keys()) | {str(slot_id).strip() for slot_id in notes.keys()}
        for slot_id_text in slot_ids:
            if not slot_id_text:
                continue
            note_text = str(notes.get(slot_id_text, "")).strip()
            image_paths = images_map.get(slot_id_text, [])
            if not note_text and not image_paths:
                continue
            rows.append(
                {
                    "school_key": school_key,
                    "college_name": college_name,
                    "team_name": team_name,
                    "zone_names": zone_name_text,
                    "slot_id": slot_id_text,
                    "slot_label": str(slot_labels.get(slot_id_text, "")).strip(),
                    "note": note_text,
                    "image_paths": json.dumps(image_paths, ensure_ascii=False),
                    "updated_at": updated_at,
                }
            )
            saved_count += 1

        write_rows(rows)
        self.send_json(
            HTTPStatus.OK,
            {
                "ok": True,
                "csvPath": csv_path_label(),
                "savedCount": saved_count,
                "schoolKey": school_key,
            },
        )

    def handle_post_note_image(self) -> None:
        try:
            payload = self.read_json_body()
            school_key = str(payload.get("schoolKey", "")).strip()
            slot_id = str(payload.get("slotId", "")).strip()
            filename = str(payload.get("filename", "")).strip()
            data_url = str(payload.get("dataUrl", "")).strip()
            if not school_key or not slot_id or not data_url:
                raise ValueError("schoolKey, slotId and dataUrl are required")
            image_path = save_note_image(school_key, slot_id, filename, data_url)
        except ValueError as exc:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except OSError as exc:
            self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"failed to write image: {exc}"})
            return

        self.send_json(
            HTTPStatus.OK,
            {
                "ok": True,
                "imagePath": image_path,
            },
        )

    def read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON: {exc.msg}") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def send_json(self, status: HTTPStatus, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_binary(
        self,
        status: HTTPStatus,
        payload: bytes,
        content_type: str,
        cache_control: str | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        if cache_control:
            self.send_header("Cache-Control", cache_control)
        self.end_headers()
        self.wfile.write(payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve RoboMaster observer locally")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)

    ensure_storage()
    httpd = ThreadingHTTPServer((args.host, args.port), RMObserverHandler)
    print(f"Serving rm observer at http://{args.host}:{args.port}/")
    print(f"CSV output: {csv_path_label()}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
