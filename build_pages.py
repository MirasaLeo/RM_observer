#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build a read-only static site for GitHub Pages."""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path

from serve_rm_observer import (
    CSV_PATH,
    NOTE_IMAGE_DIR,
    ROOT,
    build_images_map,
    build_notes_map,
    ensure_storage,
    fetch_live_bundle,
    load_rows,
)

SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
HTML_MARKER = "    <script>\n"


def build_bootstrap() -> dict:
    rows = load_rows()
    return {
        "builtAt": datetime.now().astimezone().isoformat(timespec="seconds"),
        "liveData": fetch_live_bundle(),
        "notes": build_notes_map(rows),
        "images": build_images_map(rows),
    }


def inject_bootstrap(html: str, bootstrap: dict) -> str:
    payload = json.dumps(bootstrap, ensure_ascii=False).replace("</", "<\\/")
    injection = (
        "    <script>\n"
        f"      window.__RM_OBSERVER_STATIC__ = {payload};\n"
        "    </script>\n"
        "    <script>\n"
    )
    if HTML_MARKER not in html:
        raise RuntimeError("failed to locate main script tag in index.html")
    return html.replace(HTML_MARKER, injection, 1)


def build_site() -> Path:
    ensure_storage()
    bootstrap = build_bootstrap()

    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    source_html = (ROOT / "index.html").read_text(encoding="utf-8")
    output_html = inject_bootstrap(source_html, bootstrap)
    (SITE_DIR / "index.html").write_text(output_html, encoding="utf-8")
    (SITE_DIR / ".nojekyll").write_text("", encoding="utf-8")

    if CSV_PATH.exists():
        shutil.copy2(CSV_PATH, SITE_DATA_DIR / CSV_PATH.name)
    if NOTE_IMAGE_DIR.exists():
        shutil.copytree(NOTE_IMAGE_DIR, SITE_DATA_DIR / "imgs", dirs_exist_ok=True)

    return SITE_DIR


def main() -> int:
    site_dir = build_site()
    print(f"Built static site at {site_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
