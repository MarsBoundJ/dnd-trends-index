"""
cloud_functions/variant_resolver/review_report.py
Arcane Analytics — GCP project: dnd-trends-index

Claude Review Report — Stage 3 of the variant resolution pipeline
------------------------------------------------------------------
Reads variant_staging WHERE review_status = 'pending_claude' and produces
a structured markdown report for Claude to review before Phil sees anything.

The report groups decisions into four sections:
  1. CONFIRMED VARIANTS     — Gemini high-confidence, Claude agrees
  2. NEW CONCEPTS           — Gemini flagged as new, Claude agrees
  3. CLAUDE OVERRIDES       — Claude disagrees with Gemini's decision
  4. EDGE CASES             — BG3, homebrew, UA terms needing human judgment

Claude annotates the report, updates variant_staging with claude_decision
and claude_notes, then advances review_status → 'pending_phil'.

This module is called by main.py when stage="report" is passed,
OR run standalone via generate_report() for on-demand review.
"""

import datetime
import logging
from dataclasses import dataclass

from google.cloud import bigquery

log = logging.getLogger(__name__)

PROJECT_ID    = "dnd-trends-index"
STAGING_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.variant_staging"
LIBRARY_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.concept_library"

# ---------------------------------------------------------------------------
# Edge case detection — terms that need special human attention
# ---------------------------------------------------------------------------

# These signal BG3 / video game content
_BG3_SIGNALS = {
    "baldur", "baldurs", "larian", "bg3", "astarion", "shadowheart",
    "gale", "karlach", "wyll", "halsin", "minthara", "jaheira",
    "minsc", "act", "camp", "tadpole", "illithid",
}

# These signal homebrew content
_HOMEBREW_SIGNALS = {
    "homebrew", "mercer", "colville", "kibbles", "pointy", "laserllama",
    "mcdm", "flee mortals", "tal'dorei", "critical role", "cr",
}

# These signal UA / playtest content  
_UA_SIGNALS = {
    "ua", "unearthed", "arcana", "playtest", "one dnd", "packet",
}


def _detect_edge_case(term: str, gemini_decision: str, gemini_category: str) -> str | None:
    """
    Returns an edge case label if the term needs special attention, else None.
    Labels: "BG3" | "HOMEBREW" | "UA" | "EDITION_AMBIGUOUS"
    """
    term_lower = term.lower()
    tokens = set(term_lower.split())

    if tokens & _BG3_SIGNALS:
        return "BG3"
    if tokens & _HOMEBREW_SIGNALS:
        return "HOMEBREW"
    if tokens & _UA_SIGNALS:
        return "UA"
    # Flag if Gemini said variant but category suggests it might be new
    if gemini_decision == "variant" and gemini_category:
        return "CATEGORY_MISMATCH"
    return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@dataclass
class StagingRow:
    staging_id:          str
    run_id:              str
    term:                str
    seed_keyword:        str
    fuzzy_match_concept: str | None
    fuzzy_match_score:   float | None
    gemini_decision:     str | None
    gemini_parent:       str | None
    gemini_category:     str | None
    gemini_confidence:   str | None
    gemini_reasoning:    str | None
    edge_case:           str | None   # Computed locally


def _load_pending_claude(client: bigquery.Client, run_id: str | None = None) -> list[StagingRow]:
    """Load variant_staging rows awaiting Claude review."""
    run_filter = f"AND run_id = '{run_id}'" if run_id else ""
    query = f"""
        SELECT
            staging_id,
            run_id,
            term,
            seed_keyword,
            fuzzy_match_concept,
            fuzzy_match_score,
            gemini_decision,
            gemini_parent,
            gemini_category,
            gemini_confidence,
            gemini_reasoning
        FROM `{STAGING_TABLE}`
        WHERE review_status = 'pending_claude'
        {run_filter}
        ORDER BY gemini_decision, gemini_confidence DESC, term
    """
    rows = []
    for r in client.query(query).result():
        edge_case = _detect_edge_case(
            r["term"],
            r["gemini_decision"] or "",
            r["gemini_category"] or "",
        )
        rows.append(StagingRow(
            staging_id          = r["staging_id"],
            run_id              = r["run_id"],
            term                = r["term"],
            seed_keyword        = r["seed_keyword"],
            fuzzy_match_concept = r["fuzzy_match_concept"],
            fuzzy_match_score   = r["fuzzy_match_score"],
            gemini_decision     = r["gemini_decision"],
            gemini_parent       = r["gemini_parent"],
            gemini_category     = r["gemini_category"],
            gemini_confidence   = r["gemini_confidence"],
            gemini_reasoning    = r["gemini_reasoning"],
            edge_case           = edge_case,
        ))
    log.info("Loaded %d pending_claude rows", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _score_bar(score: float | None) -> str:
    """Visual confidence bar for fuzzy match score."""
    if score is None:
        return "—"
    filled = round((score or 0) * 10)
    return f"{'█' * filled}{'░' * (10 - filled)} {score:.2f}"


def generate_markdown_report(rows: list[StagingRow], run_id: str | None) -> str:
    """
    Generate the full Claude review report as markdown.

    Sections:
      1. Summary
      2. Edge Cases (BG3 / Homebrew / UA) — reviewed first
      3. Confirmed Variants (high confidence)
      4. New Concepts
      5. Noise
      6. Low Confidence — needs extra attention
    """
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    run_label = run_id or "all pending"

    # Partition rows into sections
    edge_cases          = [r for r in rows if r.edge_case]
    high_conf_variants  = [r for r in rows if not r.edge_case
                           and r.gemini_decision == "variant"
                           and r.gemini_confidence == "high"]
    med_conf_variants   = [r for r in rows if not r.edge_case
                           and r.gemini_decision == "variant"
                           and r.gemini_confidence != "high"]
    new_concepts        = [r for r in rows if not r.edge_case
                           and r.gemini_decision == "new_concept"]
    noise               = [r for r in rows if not r.edge_case
                           and r.gemini_decision == "noise"]

    lines = []

    # ---------------------------------------------------------------------------
    # Header
    # ---------------------------------------------------------------------------
    lines += [
        "# Arcane Analytics — Variant Resolution Review Report",
        f"**Generated:** {now}",
        f"**Run:** {run_label}",
        f"**Total terms:** {len(rows)}",
        "",
        "## Summary",
        f"| Category | Count |",
        f"|---|---|",
        f"| Edge cases (BG3 / Homebrew / UA) | {len(edge_cases)} |",
        f"| High-confidence variants | {len(high_conf_variants)} |",
        f"| Medium-confidence variants | {len(med_conf_variants)} |",
        f"| New concepts | {len(new_concepts)} |",
        f"| Noise | {len(noise)} |",
        "",
        "---",
        "",
        "## Instructions for Claude",
        "For each term below, either:",
        "- ✅ **AGREE** — accept Gemini's decision as-is",
        "- 🔄 **OVERRIDE** — change the decision and add your reasoning",
        "- ⚠️ **ESCALATE** — flag for Phil's direct attention (use sparingly)",
        "",
        "After review, update variant_staging using update_claude_decisions().",
        "",
        "---",
        "",
    ]

    # ---------------------------------------------------------------------------
    # Section 1: Edge Cases
    # ---------------------------------------------------------------------------
    if edge_cases:
        lines += [
            "## ⚠️ Edge Cases — Review First",
            "_These terms triggered special signals (BG3, Homebrew, UA). Review carefully._",
            "",
        ]
        for r in edge_cases:
            lines += _format_row(r, show_edge=True)
        lines += ["---", ""]

    # ---------------------------------------------------------------------------
    # Section 2: High-confidence variants
    # ---------------------------------------------------------------------------
    if high_conf_variants:
        lines += [
            "## ✅ High-Confidence Variants",
            "_Gemini is confident these are search variants of existing concepts._",
            "_Spot-check a sample — if they look right, bulk-approve._",
            "",
        ]
        for r in high_conf_variants:
            lines += _format_row(r)
        lines += ["---", ""]

    # ---------------------------------------------------------------------------
    # Section 3: Medium-confidence variants
    # ---------------------------------------------------------------------------
    if med_conf_variants:
        lines += [
            "## 🔍 Medium-Confidence Variants",
            "_Gemini thinks these are variants but with lower confidence. Review each one._",
            "",
        ]
        for r in med_conf_variants:
            lines += _format_row(r)
        lines += ["---", ""]

    # ---------------------------------------------------------------------------
    # Section 4: New concepts
    # ---------------------------------------------------------------------------
    if new_concepts:
        lines += [
            "## 🆕 New Concepts",
            "_Gemini flagged these as genuinely new — not variants of anything existing._",
            "_Verify the suggested category is correct before approving._",
            "",
        ]
        for r in new_concepts:
            lines += _format_row(r)
        lines += ["---", ""]

    # ---------------------------------------------------------------------------
    # Section 5: Noise
    # ---------------------------------------------------------------------------
    if noise:
        lines += [
            "## 🗑️ Noise",
            "_Gemini flagged these as non-meaningful D&D search signals._",
            "_Skim for false positives — Gemini occasionally over-filters._",
            "",
        ]
        for r in noise:
            lines += _format_row(r)
        lines += ["---", ""]

    lines += [
        "",
        "---",
        "_End of report. After Claude review, run update_claude_decisions() to advance rows to pending_phil._",
    ]

    return "\n".join(lines)


def _format_row(row: StagingRow, show_edge: bool = False) -> list[str]:
    """Format a single staging row for the report."""
    lines = []

    decision_emoji = {
        "variant":     "🔀",
        "new_concept": "🆕",
        "noise":       "🗑️",
    }.get(row.gemini_decision or "", "❓")

    conf_emoji = {
        "high":   "🟢",
        "medium": "🟡",
        "low":    "🔴",
    }.get(row.gemini_confidence or "", "⚪")

    lines.append(f"### {decision_emoji} `{row.term}`")

    if show_edge and row.edge_case:
        lines.append(f"> **⚠️ Edge case: {row.edge_case}**")

    lines.append(f"- **Seed keyword:** {row.seed_keyword}")

    if row.fuzzy_match_concept:
        lines.append(f"- **Fuzzy match:** {row.fuzzy_match_concept} {_score_bar(row.fuzzy_match_score)}")
    else:
        lines.append(f"- **Fuzzy match:** none")

    lines.append(f"- **Gemini decision:** {decision_emoji} {row.gemini_decision}")

    if row.gemini_parent:
        lines.append(f"- **Parent concept:** {row.gemini_parent}")
    if row.gemini_category:
        lines.append(f"- **Suggested category:** {row.gemini_category}")

    lines.append(f"- **Confidence:** {conf_emoji} {row.gemini_confidence}")
    lines.append(f"- **Gemini reasoning:** _{row.gemini_reasoning}_")
    lines.append(f"- **Claude review:** [ ] AGREE  [ ] OVERRIDE  [ ] ESCALATE")
    lines.append(f"- **Claude notes:** _(add if overriding)_")
    lines.append("")

    return lines


# ---------------------------------------------------------------------------
# Write Claude decisions back to BigQuery
# ---------------------------------------------------------------------------

def update_claude_decisions(
    client: bigquery.Client,
    decisions: list[dict],
) -> int:
    """
    Write Claude's review decisions back to variant_staging.
    Advances review_status from 'pending_claude' → 'pending_phil'.

    decisions: list of dicts with keys:
        staging_id    : str
        claude_decision : "agree" | "override"
        claude_override : str | None  (required if decision = "override")
        claude_notes    : str | None
    
    Returns count of rows updated.
    """
    now = datetime.datetime.utcnow().isoformat()
    updated = 0

    for d in decisions:
        def _esc(s):
            return s.replace("'", "\\'") if s else ""

        staging_id      = d["staging_id"]
        claude_decision = _esc(d.get("claude_decision", "agree"))
        claude_override = _esc(d.get("claude_override") or "")
        claude_notes    = _esc(d.get("claude_notes") or "")

        override_sql = f"'{claude_override}'" if claude_override else "NULL"
        notes_sql    = f"'{claude_notes}'"    if claude_notes    else "NULL"

        query = f"""
            UPDATE `{STAGING_TABLE}`
            SET
                claude_decision = '{claude_decision}',
                claude_override = {override_sql},
                claude_notes    = {notes_sql},
                review_status   = 'pending_phil',
                updated_at      = '{now}'
            WHERE staging_id = '{staging_id}'
        """
        client.query(query).result()
        updated += 1

    log.info("Updated %d rows → pending_phil", updated)
    return updated


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_report(
    client: bigquery.Client,
    run_id: str | None = None,
    output_path: str | None = None,
) -> str:
    """
    Generate the Claude review report.

    Args:
        client:      BigQuery client
        run_id:      Optional — filter to specific run
        output_path: Optional — write report to this GCS path or local path

    Returns:
        The report as a markdown string.
    """
    rows = _load_pending_claude(client, run_id)

    if not rows:
        log.info("No pending_claude rows — nothing to report")
        return "# No terms pending Claude review."

    report = generate_markdown_report(rows, run_id)

    if output_path:
        if output_path.startswith("gs://"):
            from google.cloud import storage
            bucket_name, blob_path = output_path[5:].split("/", 1)
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(report, content_type="text/markdown")
            log.info("Report written to %s", output_path)
        else:
            with open(output_path, "w") as f:
                f.write(report)
            log.info("Report written to %s", output_path)

    return report
