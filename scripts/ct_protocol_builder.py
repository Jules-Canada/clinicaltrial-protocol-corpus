#!/usr/bin/env python3
"""
ct_protocol_builder.py

Phase 1: scan ClinicalTrials.gov API for protocol PDFs   (single-threaded, rate-limited)
Phase 2: download and extract text across N workers       (parallel, each with own session)

Usage:
    python scripts/ct_protocol_builder.py --sample 50 --workers 4
"""

import argparse
import io
import json
import time
from datetime import date
from multiprocessing import Pool
from pathlib import Path

import re

import fitz  # pymupdf
import requests
from tqdm import tqdm

API_BASE   = "https://clinicaltrials.gov/api/v2/studies"
CDN_BASE   = "https://cdn.clinicaltrials.gov/large-docs"
USER_AGENT = "protocol-corpus/1.0 (public research dataset)"
API_DELAY  = 1.0  # robots.txt crawl-delay for clinicaltrials.gov
PDF_DELAY  = 0.2  # CDN (Google Cloud Storage) — no stated limit

OUTPUT_DIR = Path("ct_corpus")
PDF_DIR    = OUTPUT_DIR / "pdfs"
JSONL_PATH = OUTPUT_DIR / "protocols.jsonl"


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def load_checkpoint(jsonl_path):
    """Return (processed_nct_ids, processed_urls) from existing output."""
    if not jsonl_path.exists():
        return set(), set()
    nct_ids, urls = set(), set()
    with open(jsonl_path) as f:
        for line in f:
            try:
                r = json.loads(line)
                nct_ids.add(r["nct_id"])
                urls.add(r["source_url"])
            except (json.JSONDecodeError, KeyError):
                pass
    return nct_ids, urls


# ---------------------------------------------------------------------------
# Phase 1 — API scan (single-threaded)
# ---------------------------------------------------------------------------

def fetch_page(session, params):
    resp = session.get(API_BASE, params=params, timeout=30)
    if resp.status_code == 429:
        raise RuntimeError("Rate limited (429) — try again later")
    resp.raise_for_status()
    return resp.json()


def protocol_docs(study):
    docs = (
        study.get("documentSection", {})
             .get("largeDocumentModule", {})
             .get("largeDocs", [])
    )
    return [d for d in docs if d.get("hasProtocol")]


def flatten_metadata(study):
    proto       = study.get("protocolSection", {})
    id_mod      = proto.get("identificationModule", {})
    design_mod  = proto.get("designModule", {})
    cond_mod    = proto.get("conditionsModule", {})
    sponsor_mod = proto.get("sponsorCollaboratorsModule", {})
    phases      = design_mod.get("phases", [])
    return {
        "nct_id":     id_mod.get("nctId", ""),
        "title":      id_mod.get("officialTitle") or id_mod.get("briefTitle", ""),
        "phase":      phases[0] if phases else "",
        "conditions": cond_mod.get("conditions", []),
        "sponsor":    sponsor_mod.get("leadSponsor", {}).get("name", ""),
    }


def cdn_url(nct_id, filename):
    return f"{CDN_BASE}/{nct_id[-2:]}/{nct_id}/{filename}"


def scan_for_work_items(session, limit, skip_nct_ids, skip_urls):
    """
    Walk API pages and return a list of work items — one per protocol document.
    Skips NCT IDs and URLs already in the checkpoint.
    """
    work_items = []
    page_token = None

    with tqdm(desc="Phase 1  scanning API", unit=" docs", postfix={"studied": 0}) as pbar:
        studies_scanned = 0
        while len(work_items) < limit:
            params = {"pageSize": 100, "format": "json"}
            if page_token:
                params["pageToken"] = page_token

            data = fetch_page(session, params)

            for study in data.get("studies", []):
                studies_scanned += 1
                meta   = flatten_metadata(study)
                nct_id = meta["nct_id"]

                if nct_id in skip_nct_ids:
                    continue

                for doc in protocol_docs(study):
                    filename = doc.get("filename", "")
                    if not filename:
                        continue
                    url = cdn_url(nct_id, filename)
                    if url in skip_urls:
                        continue
                    work_items.append({**meta, "url": url, "filename": filename})
                    pbar.update(1)
                    pbar.set_postfix({"studied": studies_scanned})

                if len(work_items) >= limit:
                    break

            page_token = data.get("nextPageToken")
            if not page_token:
                break

            time.sleep(API_DELAY)

    return work_items[:limit]


# ---------------------------------------------------------------------------
# Phase 2 — parallel download + extract
# ---------------------------------------------------------------------------

def extract_text(source):
    """Extract text from a PDF path, BytesIO, or raw bytes. Returns (page_count, full_text)."""
    name = source.name if isinstance(source, Path) else "in-memory"
    try:
        if isinstance(source, Path):
            doc = fitz.open(str(source))
        else:
            data = source.read() if hasattr(source, "read") else source
            doc = fitz.open(stream=data, filetype="pdf")
        with doc:
            pages = [doc[i].get_text() for i in range(len(doc))]
        full_text = re.sub(r"\n{3,}", "\n\n", "\n\n".join(pages))
        return len(pages), full_text
    except Exception as e:
        print(f"  [warn] text extraction failed for {name}: {e}")
        return 0, ""


def fetch_and_extract(session, url, save_path=None):
    """Download a PDF and extract text. Optionally cache to disk."""
    if save_path and save_path.exists():
        return True, *extract_text(save_path)

    resp = session.get(url, timeout=60)
    if resp.status_code != 200:
        return False, 0, ""

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(resp.content)
        return True, *extract_text(save_path)

    return True, *extract_text(resp.content)


def process_work_item(item):
    """Worker entry point — each subprocess creates its own HTTP session."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    nct_id    = item["nct_id"]
    url       = item["url"]
    save_path = Path(PDF_DIR / nct_id / item["filename"]) if item.get("save_pdfs") else None

    time.sleep(PDF_DELAY)
    ok, page_count, full_text = fetch_and_extract(session, url, save_path)
    if not ok:
        return None

    text_source = "needs_ocr" if (page_count > 0 and not full_text.strip()) else "digital"

    return {
        "nct_id":         nct_id,
        "title":          item["title"],
        "phase":          item["phase"],
        "conditions":     item["conditions"],
        "sponsor":        item["sponsor"],
        "source_url":     url,
        "retrieved_date": date.today().isoformat(),
        "page_count":     page_count,
        "text_source":    text_source,
        "sections":       {},
        "full_text":      full_text,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build a clinical trial protocol corpus")
    parser.add_argument("--sample",    type=int, default=50, help="Number of protocol docs to fetch")
    parser.add_argument("--workers",   type=int, default=1,  help="Parallel download workers (default: 1)")
    parser.add_argument("--save-pdfs", action="store_true",  help="Cache PDFs to disk (dev/testing only)")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)
    if args.save_pdfs:
        PDF_DIR.mkdir(exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    processed_nct_ids, processed_urls = load_checkpoint(JSONL_PATH)
    if processed_nct_ids:
        print(f"Resuming — {len(processed_nct_ids):,} docs in checkpoint ({len(processed_nct_ids)/581764*100:.1f}% of ~581k studies)\n")

    print(f"Phase 1: scanning for {args.sample} protocol documents...")
    work_items = scan_for_work_items(session, args.sample, processed_nct_ids, processed_urls)
    for item in work_items:
        item["save_pdfs"] = args.save_pdfs
    print(f"  Queued {len(work_items)} documents\n")

    print(f"Phase 2: downloading + extracting ({args.workers} worker{'s' if args.workers > 1 else ''})...")
    records_written = 0

    with open(JSONL_PATH, "a") as out:
        with Pool(args.workers) as pool:
            results = pool.imap_unordered(process_work_item, work_items)
            for record in tqdm(results, total=len(work_items), desc="Phase 2  processing"):
                if record:
                    out.write(json.dumps(record) + "\n")
                    out.flush()
                    records_written += 1

    print(f"\nDone. {records_written} new records written to {JSONL_PATH}")


if __name__ == "__main__":
    main()
