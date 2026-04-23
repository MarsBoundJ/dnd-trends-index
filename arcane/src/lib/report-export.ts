/*
 * Arcane Analytics — Bag of Holding report export helpers.
 *
 * Client-side serializers that turn an organized bag (items + clusters
 * from the /api/bag/organize endpoint) into a shareable text artifact
 * in one of two formats — Markdown or plain text — and trigger a
 * browser download via Blob URL.
 *
 * Why client-side: the browser already has all the data (bag items in
 * Zustand, clusters in sessionStorage) so serializing server-side would
 * just add a round-trip. Blob-based downloads are a 30-year-old stable
 * browser API, works across every modern browser.
 *
 * The third format — PDF — is handled by window.print() on the report
 * page itself; no serializer needed, the browser's own "Save as PDF"
 * dialog renders the page with the @print CSS stylesheet.
 */

import type {
  StowedCard,
  StowedSageAnswer,
  StowedSageThread,
  StowedItem,
} from "@/lib/bag-store"

// ─── Shared types ────────────────────────────────────────────────────────────

/**
 * A single section in the organized report. Matches the shape returned
 * by /api/bag/organize's structured output, plus the resolved items
 * (looked up by id on the client from the Zustand bag).
 */
export interface ReportSection {
  title: string
  description?: string
  items: StowedItem[]
}

/**
 * Report-level metadata — title, date, author — rendered at the top
 * of every export format.
 */
export interface ReportMeta {
  /** Report title. Defaults to "Arcane Analytics — Bag of Holding Report". */
  title?: string
  /** The user's display name for a "prepared for" line. Optional. */
  preparedFor?: string
  /** The user's prompt/instructions (if any) for transparency. Optional. */
  instructions?: string
  /** ISO date string — defaults to today. */
  generatedAt?: string
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Today's date as "April 23, 2026" — locale-aware, no timezone surprises.
 */
function formatDate(iso?: string): string {
  const d = iso ? new Date(iso) : new Date()
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  })
}

/**
 * Derive a short title for rendering any stowed item in a report.
 * Falls back gracefully for the three kinds.
 */
function itemHeader(item: StowedItem): string {
  if (item.kind === "card") {
    return item.title
  }
  if (item.kind === "sage-answer") {
    // Truncate long questions — they can get long-winded.
    return item.question.length > 80
      ? item.question.slice(0, 77) + "..."
      : item.question
  }
  // sage-thread — use the first user message as a title proxy.
  const firstUser = item.messages.find((m) => m.role === "user")
  return firstUser
    ? firstUser.text.length > 80
      ? firstUser.text.slice(0, 77) + "..."
      : firstUser.text
    : "Untitled Sage conversation"
}

/**
 * Derive the human-readable body content for each item kind.
 */
function itemBody(item: StowedItem): string {
  if (item.kind === "card") {
    return item.snapshot
  }
  if (item.kind === "sage-answer") {
    return item.answer
  }
  // sage-thread — flatten messages into a transcript.
  return item.messages
    .map((m) => `${m.role === "user" ? "User" : "Sage"}: ${m.text}`)
    .join("\n\n")
}

/**
 * Subtitle or metadata line to render under each item's header.
 * Empty string if nothing useful to show.
 */
function itemSubtitle(item: StowedItem): string {
  if (item.kind === "card") {
    const parts = [item.subtitle, item.lens, item.cardType].filter(Boolean)
    return parts.length > 0 ? parts.join(" · ") : ""
  }
  if (item.kind === "sage-answer") {
    return item.pageContext ? `Context: ${item.pageContext}` : ""
  }
  return "Sage conversation thread"
}

// ─── Markdown serializer ─────────────────────────────────────────────────────

/**
 * Serialize an organized bag into Markdown. Designed to paste cleanly
 * into Notion, Obsidian, Linear, GitHub — anywhere markdown renders.
 */
export function serializeToMarkdown(
  sections: ReportSection[],
  meta: ReportMeta = {},
): string {
  const title = meta.title ?? "Arcane Analytics — Bag of Holding Report"
  const date = formatDate(meta.generatedAt)

  const lines: string[] = []

  // ── Title block ────────────────────────────────────────────────────────
  lines.push(`# ${title}`)
  lines.push("")

  const byLine = [
    `*Generated ${date}*`,
    meta.preparedFor ? `*Prepared for ${meta.preparedFor}*` : null,
  ]
    .filter(Boolean)
    .join(" · ")
  lines.push(byLine)
  lines.push("")

  if (meta.instructions) {
    lines.push(`> **Organization brief:** ${meta.instructions}`)
    lines.push("")
  }

  lines.push("---")
  lines.push("")

  // ── Sections ──────────────────────────────────────────────────────────
  for (const section of sections) {
    lines.push(`## ${section.title}`)
    lines.push("")

    if (section.description) {
      lines.push(`*${section.description}*`)
      lines.push("")
    }

    for (const item of section.items) {
      lines.push(`### ${itemHeader(item)}`)

      const subtitle = itemSubtitle(item)
      if (subtitle) {
        lines.push(`*${subtitle}*`)
      }
      lines.push("")
      lines.push(itemBody(item))
      lines.push("")
    }
  }

  return lines.join("\n")
}

// ─── Plain text serializer ───────────────────────────────────────────────────

/**
 * Serialize to plain text. Suitable for email bodies, Slack messages,
 * Word documents — anywhere formatting might be stripped or clash.
 *
 * Uses underline-style headers (`===` for title, `---` for sections)
 * which render naturally even in environments without markdown support.
 */
export function serializeToPlainText(
  sections: ReportSection[],
  meta: ReportMeta = {},
): string {
  const title = meta.title ?? "Arcane Analytics — Bag of Holding Report"
  const date = formatDate(meta.generatedAt)

  const lines: string[] = []

  // ── Title block ────────────────────────────────────────────────────────
  lines.push(title)
  lines.push("=".repeat(Math.min(title.length, 80)))
  lines.push("")

  lines.push(`Generated ${date}`)
  if (meta.preparedFor) {
    lines.push(`Prepared for ${meta.preparedFor}`)
  }
  lines.push("")

  if (meta.instructions) {
    lines.push(`Organization brief: ${meta.instructions}`)
    lines.push("")
  }

  // ── Sections ──────────────────────────────────────────────────────────
  for (const section of sections) {
    lines.push("")
    lines.push(section.title)
    lines.push("-".repeat(Math.min(section.title.length, 80)))
    lines.push("")

    if (section.description) {
      lines.push(section.description)
      lines.push("")
    }

    for (const item of section.items) {
      lines.push(`  ${itemHeader(item)}`)

      const subtitle = itemSubtitle(item)
      if (subtitle) {
        lines.push(`  (${subtitle})`)
      }
      lines.push("")
      lines.push(
        itemBody(item)
          .split("\n")
          .map((l) => `  ${l}`)
          .join("\n"),
      )
      lines.push("")
    }
  }

  return lines.join("\n")
}

// ─── Download trigger ────────────────────────────────────────────────────────

/**
 * Trigger a browser download of the given string content under the given
 * filename + MIME type. Works via the Blob URL pattern — universally
 * supported, no external libraries needed.
 *
 * Side effect: appends a hidden <a> to the document body, clicks it,
 * removes it. Standard pattern for programmatic downloads.
 */
export function downloadTextFile(
  content: string,
  filename: string,
  mimeType: "text/markdown" | "text/plain",
): void {
  const blob = new Blob([content], { type: `${mimeType};charset=utf-8` })
  const url = URL.createObjectURL(blob)

  const a = document.createElement("a")
  a.href = url
  a.download = filename
  a.style.display = "none"
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)

  // Release the blob URL on the next tick — gives the browser time to
  // initiate the download before the URL becomes invalid.
  setTimeout(() => URL.revokeObjectURL(url), 100)
}

/**
 * Convenience filename helper — "arcane-report-2026-04-23.md"
 */
export function defaultFilename(
  extension: "md" | "txt",
  generatedAt?: string,
): string {
  const d = generatedAt ? new Date(generatedAt) : new Date()
  const iso = d.toISOString().slice(0, 10) // YYYY-MM-DD
  return `arcane-report-${iso}.${extension}`
}

// ─── Typed helpers for item-id lookup (used by the report page) ─────────────

/**
 * Given a flat list of bag items and the cluster output from
 * /api/bag/organize, resolve the clusters into ReportSection[] with
 * the items looked up by id. Drops any ids that don't resolve (shouldn't
 * happen, but belt-and-suspenders after the endpoint's own sanity check).
 */
export function resolveClustersToSections(
  items: StowedItem[],
  clusters: Array<{ title: string; description?: string; itemIds: string[] }>,
): ReportSection[] {
  const byId = new Map(items.map((i) => [i.id, i]))

  return clusters.map((cluster) => ({
    title: cluster.title,
    description: cluster.description,
    items: cluster.itemIds
      .map((id) => byId.get(id))
      .filter((item): item is StowedItem => item !== undefined),
  }))
}

/**
 * Fallback organization when the user clicks Generate Report without
 * invoking Sage — or when Sage fails. Groups by item kind (Cards first,
 * then Sage Answers, then Sage Threads). Simple but always works.
 */
export function defaultOrganizationByKind(
  items: StowedItem[],
): ReportSection[] {
  const cards = items.filter((i): i is StowedCard => i.kind === "card")
  const answers = items.filter(
    (i): i is StowedSageAnswer => i.kind === "sage-answer",
  )
  const threads = items.filter(
    (i): i is StowedSageThread => i.kind === "sage-thread",
  )

  const sections: ReportSection[] = []
  if (cards.length > 0) {
    sections.push({ title: "Cards", items: cards })
  }
  if (answers.length > 0) {
    sections.push({ title: "Sage Answers", items: answers })
  }
  if (threads.length > 0) {
    sections.push({ title: "Sage Threads", items: threads })
  }
  return sections
}
