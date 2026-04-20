"""
Arcane Analytics — Hasbro 2026 frame corpus ingestion (Step 9.8, Chunk B).

Chunks the four Hasbro corporate documents and writes each chunk to
`frames/hasbro-2026/corpus/{chunk_id}` for later retrieval by the
journalist prompt assembler (Chunk D) and Sage frame-context injection
(Chunk E, eventual 9.8b).

Source PDFs (in ~/Downloads/):
  - 2025 Annual Report FINAL 04172026.pdf
  - Hasbro Reports Fourth Quarter and Full Year 2025 Financial Results.pdf
  - Hasbro Form 10 K Feb 2026.pdf
  - Hasbro Unveils New Strategy – Playing to Win.pdf

Chunk schema:
    {
        "chunk_id": str,           # deterministic: f"{doc_key}_chunk_{idx:04d}"
        "source_doc": str,          # short key (e.g. "annual_report_2025")
        "source_label": str,        # human label for citation prose
        "page_range": str,          # e.g. "12-14" or "7" if single-page
        "section": str | None,      # heuristic heading if detectable
        "topic_tags": list[str],    # keyword-matched tag list
        "text": str,                # the chunk content (~500 words target)
        "word_count": int,          # post-chunk word count
    }

Chunking strategy:
  - pdftotext emits `\f` (form feed) between pages — we split on that to
    preserve per-page text, then aggregate across pages up to a target
    word count (default 500), flushing a chunk when the target is hit.
  - Page boundaries are honored for `page_range`; chunks never slice
    inside a paragraph where avoidable.

Run:
    python setup_hasbro_frame_corpus.py
    python setup_hasbro_frame_corpus.py --dry-run  # prints chunk counts, no writes

Safety:
  - Idempotent via set(merge=True) on deterministic chunk_ids.
  - Re-running overwrites with identical content; safe.
  - Uses `-enc UTF-8` on pdftotext so en-dash / em-dash render cleanly
    (pdftotext default is Latin1 which mangles Unicode punctuation).
  - Requires ADC: `gcloud auth application-default login`.
  - Requires pdftotext in PATH (installs with Git for Windows / MSYS2).
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from google.cloud import firestore

PROJECT_ID = "dnd-trends-index"
FRAMES_COLLECTION = "frames"
FRAME_ID = "hasbro-2026"
CORPUS_SUBCOLLECTION = "corpus"

DOWNLOADS = Path.home() / "Downloads"

# (doc_key, filename, human_label). doc_key becomes the chunk_id prefix.
SOURCES: list[tuple[str, str, str]] = [
    (
        "annual_report_2025",
        "2025 Annual Report FINAL 04172026.pdf",
        "Hasbro 2025 Annual Report",
    ),
    (
        "q4_2025_earnings",
        "Hasbro Reports Fourth Quarter and Full Year 2025 Financial Results.pdf",
        "Hasbro Q4 2025 Earnings",
    ),
    (
        "form_10k_feb_2026",
        "Hasbro Form 10 K Feb 2026.pdf",
        "Hasbro Form 10-K (Feb 2026)",
    ),
    (
        "playing_to_win_press",
        "Hasbro Unveils New Strategy – Playing to Win.pdf",
        "Hasbro Unveils New Strategy — Playing to Win",
    ),
]

TARGET_WORDS_PER_CHUNK = 500
MIN_WORDS_TO_EMIT = 50  # Below this, swallow the tail into the previous chunk.

# Topic-tag heuristics. Each tag matches if the regex hits anywhere in
# the chunk text (case-insensitive). Order doesn't matter — tags are a
# retrieval hint, not a taxonomy.
TOPIC_TAG_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("tariffs", re.compile(r"\btariff", re.I)),
    ("universes_beyond", re.compile(r"universes\s+beyond", re.I)),
    ("dnd", re.compile(r"\bdungeons\s*&\s*dragons\b|\bd&d\b|\bdnd\b", re.I)),
    ("magic", re.compile(r"\bmagic:\s*the\s+gathering\b|\bmtg\b", re.I)),
    ("digital", re.compile(r"\bdigital\b", re.I)),
    ("playing_to_win", re.compile(r"playing\s+to\s+win", re.I)),
    ("grow_brands", re.compile(r"\bgrow\s+brand", re.I)),
    ("optimize_brands", re.compile(r"\boptimize\s+brand", re.I)),
    ("reinvent_brands", re.compile(r"\breinvent\s+brand", re.I)),
    ("gem2", re.compile(r"GEM\s*²|GEM2|GEM\^2", re.I)),
    ("aging_up", re.compile(r"aging\s+up", re.I)),
    ("exodus", re.compile(r"\bEXODUS\b")),
    ("warlock", re.compile(r"\bWARLOCK\b", re.I)),
    ("monopoly_go", re.compile(r"MONOPOLY\s+GO", re.I)),
    ("scopely", re.compile(r"\bscopely\b", re.I)),
    ("pulse", re.compile(r"\bhasbro\s+pulse\b", re.I)),
    ("secret_lair", re.compile(r"secret\s+lair", re.I)),
    ("wpn", re.compile(r"\bwizards\s+play\s+network\b|\bwpn\b", re.I)),
    ("licensing", re.compile(r"\blicensing\b|\blicensed\b", re.I)),
    ("transformers", re.compile(r"\btransformers\b", re.I)),
    ("monopoly", re.compile(r"\bmonopoly\b", re.I)),
    ("play_doh", re.compile(r"play-?doh", re.I)),
    ("goodwill", re.compile(r"\bgoodwill\b", re.I)),
    ("impairment", re.compile(r"\bimpairment\b", re.I)),
    ("cost_savings", re.compile(r"cost\s+saving", re.I)),
    ("operating_margin", re.compile(r"operating\s+margin", re.I)),
    ("bg3_baldurs_gate", re.compile(r"baldur", re.I)),
    ("partnership", re.compile(r"\bpartner(ship)?\b", re.I)),
    ("d2c", re.compile(r"direct[\s-]to[\s-]consumer|\bd2c\b", re.I)),
]


@dataclass
class Chunk:
    chunk_id: str
    source_doc: str
    source_label: str
    page_range: str
    section: str | None
    topic_tags: list[str]
    text: str
    word_count: int


def extract_pages(pdf_path: Path) -> list[str]:
    """Return a list of page-text strings, preserving page boundaries.

    pdftotext emits `\\f` (form feed) between pages when producing
    plain text. We split on that so downstream chunking can record
    page_range accurately.
    """
    if not shutil.which("pdftotext"):
        raise RuntimeError(
            "pdftotext not found on PATH. Install Git for Windows "
            "(bundles pdftotext) or MSYS2, then retry."
        )
    result = subprocess.run(
        ["pdftotext", "-enc", "UTF-8", "-layout", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    # Trailing form feed on the last page is common; filter empty tail.
    pages = result.stdout.split("\f")
    return [p for p in pages if p.strip()]


def heuristic_section(chunk_text: str) -> str | None:
    """Return the first short, heading-shaped line of the chunk, if any.

    A "heading" is a line <= 80 chars that's either Title Case or
    mostly uppercase, with at least one letter. Very forgiving —
    imperfect matches are fine; the field is a hint, not a contract.
    """
    for raw in chunk_text.splitlines():
        line = raw.strip()
        if not line or len(line) > 80:
            continue
        alpha = [c for c in line if c.isalpha()]
        if len(alpha) < 3:
            continue
        upper_ratio = sum(1 for c in alpha if c.isupper()) / len(alpha)
        if upper_ratio >= 0.7:
            return line
        # Title Case heuristic: every word starts with upper, no ending punct.
        words = line.split()
        if len(words) >= 2 and all(
            w[0].isupper() or not w[0].isalpha() for w in words
        ) and not line.endswith((".", ",", ";", ":")):
            return line
        return None  # First non-heading line means no section.
    return None


def tag_topics(text: str) -> list[str]:
    tags = [tag for tag, pat in TOPIC_TAG_PATTERNS if pat.search(text)]
    return tags


def chunk_pages(
    pages: list[str],
    doc_key: str,
    source_label: str,
    target_words: int = TARGET_WORDS_PER_CHUNK,
) -> list[Chunk]:
    """Aggregate pages into ~target_words chunks, honoring page boundaries.

    A chunk starts when we emit one and ends when accumulated words
    >= target. We never split a page across chunks — if adding the next
    page would push us much past target, we flush first, unless the
    accumulated chunk is empty (giant pages become their own chunk).
    """
    chunks: list[Chunk] = []
    buf: list[str] = []
    buf_words = 0
    start_page = 1
    current_page = 0

    for i, page_text in enumerate(pages, start=1):
        page_words = len(page_text.split())
        if buf and buf_words + page_words > target_words * 1.4:
            # Flush — this page would overshoot; keep existing chunk
            # intact.
            chunks.append(
                _make_chunk(
                    buf, doc_key, source_label, start_page, current_page,
                    len(chunks),
                )
            )
            buf = []
            buf_words = 0
            start_page = i

        if not buf:
            start_page = i
        buf.append(page_text)
        buf_words += page_words
        current_page = i

        if buf_words >= target_words:
            chunks.append(
                _make_chunk(
                    buf, doc_key, source_label, start_page, current_page,
                    len(chunks),
                )
            )
            buf = []
            buf_words = 0

    # Tail: emit if substantial, otherwise merge into the previous chunk.
    if buf:
        if buf_words >= MIN_WORDS_TO_EMIT or not chunks:
            chunks.append(
                _make_chunk(
                    buf, doc_key, source_label, start_page, current_page,
                    len(chunks),
                )
            )
        else:
            prev = chunks[-1]
            combined_text = prev.text + "\n\n" + "\n\n".join(buf)
            prev.text = combined_text
            prev.word_count = len(combined_text.split())
            prev.page_range = _fmt_range(
                int(prev.page_range.split("-")[0]), current_page
            )
            prev.topic_tags = sorted(
                set(prev.topic_tags) | set(tag_topics(combined_text))
            )

    return chunks


def _make_chunk(
    page_texts: Iterable[str],
    doc_key: str,
    source_label: str,
    start_page: int,
    end_page: int,
    chunk_idx: int,
) -> Chunk:
    text = "\n\n".join(p.strip() for p in page_texts if p.strip())
    return Chunk(
        chunk_id=f"{doc_key}_chunk_{chunk_idx:04d}",
        source_doc=doc_key,
        source_label=source_label,
        page_range=_fmt_range(start_page, end_page),
        section=heuristic_section(text),
        topic_tags=tag_topics(text),
        text=text,
        word_count=len(text.split()),
    )


def _fmt_range(start: int, end: int) -> str:
    return str(start) if start == end else f"{start}-{end}"


def write_chunks(
    db: firestore.Client, chunks: list[Chunk], dry_run: bool
) -> None:
    coll = (
        db.collection(FRAMES_COLLECTION)
        .document(FRAME_ID)
        .collection(CORPUS_SUBCOLLECTION)
    )
    if dry_run:
        print(f"[DRY-RUN] would write {len(chunks)} chunks")
        return
    # Batched writes — Firestore caps batches at 500 ops; we chunk in 400s.
    BATCH_SIZE = 400
    for start in range(0, len(chunks), BATCH_SIZE):
        batch = db.batch()
        for chunk in chunks[start : start + BATCH_SIZE]:
            batch.set(
                coll.document(chunk.chunk_id),
                {
                    "chunk_id": chunk.chunk_id,
                    "source_doc": chunk.source_doc,
                    "source_label": chunk.source_label,
                    "page_range": chunk.page_range,
                    "section": chunk.section,
                    "topic_tags": chunk.topic_tags,
                    "text": chunk.text,
                    "word_count": chunk.word_count,
                },
                merge=True,
            )
        batch.commit()
        print(
            f"[OK] committed batch {start // BATCH_SIZE + 1} "
            f"({min(BATCH_SIZE, len(chunks) - start)} chunks)"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse + chunk but don't write to Firestore.",
    )
    args = parser.parse_args()

    all_chunks: list[Chunk] = []
    for doc_key, filename, source_label in SOURCES:
        pdf_path = DOWNLOADS / filename
        if not pdf_path.exists():
            print(f"[ERR] missing: {pdf_path}", file=sys.stderr)
            sys.exit(1)
        print(f"[..] extracting {filename}")
        pages = extract_pages(pdf_path)
        print(f"     {len(pages)} pages")
        chunks = chunk_pages(pages, doc_key, source_label)
        print(
            f"[OK] {doc_key}: {len(chunks)} chunks "
            f"(avg {sum(c.word_count for c in chunks) // max(1, len(chunks))} words)"
        )
        all_chunks.extend(chunks)

    print(f"\nTotal chunks: {len(all_chunks)}")
    if not all_chunks:
        print("[ERR] nothing to write", file=sys.stderr)
        sys.exit(1)

    db = firestore.Client(project=PROJECT_ID)
    write_chunks(db, all_chunks, dry_run=args.dry_run)
    print("Done.")


if __name__ == "__main__":
    main()
