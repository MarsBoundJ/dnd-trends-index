"use client"

/*
 * Trusight — Bag of Holding report view.
 *
 * Displays the user's stowed items organized into thematic sections,
 * with three export paths: PDF (via browser print → Save as PDF),
 * Markdown (direct download), Plain text (direct download).
 *
 * Source of data:
 *   - Items: Zustand bag store (localStorage / Firestore sync)
 *   - Cluster organization: sessionStorage under REPORT_STATE_SESSION_KEY
 *     (freshly written by GenerateReportForm on /collection)
 *
 * Fallback path: if no cluster organization is in sessionStorage (user
 * navigated here directly, or session was wiped), falls back to the
 * default content-kind grouping (Cards / Sage Answers / Sage Threads)
 * so the page never crashes on a missing handoff.
 *
 * Print optimization: @media print rules in globals.css hide the site
 * header, the export-buttons bar, and the "Back to bag" link. They
 * also swap the dark palette to a print-friendly light palette so
 * the PDF output is readable on paper without eating toner.
 */

import { useEffect, useState, useMemo } from "react"
import Link from "next/link"
import { useSession } from "next-auth/react"
import {
  FileText,
  FileType,
  Printer,
  ArrowLeft,
  Sparkles,
  Bookmark,
  MessageSquare,
} from "lucide-react"

import {
  useBagStore,
  useBagHasHydrated,
  type StowedCard,
  type StowedSageAnswer,
  type StowedSageThread,
  type StowedItem,
} from "@/lib/bag-store"
import {
  serializeToMarkdown,
  serializeToPlainText,
  downloadTextFile,
  defaultFilename,
  resolveClustersToSections,
  defaultOrganizationByKind,
  type ReportSection,
  type ReportMeta,
} from "@/lib/report-export"
import {
  REPORT_STATE_SESSION_KEY,
  type ReportStateHandoff,
} from "@/components/generate-report-form"
import { cn } from "@/lib/utils"

export default function ReportPage() {
  const hydrated = useBagHasHydrated()
  const items = useBagStore((s) => s.items)
  const { data: session } = useSession()

  const [handoff, setHandoff] = useState<ReportStateHandoff | null>(null)
  const [handoffLoaded, setHandoffLoaded] = useState(false)

  // Load the cluster handoff from sessionStorage once on mount.
  useEffect(() => {
    try {
      const raw = sessionStorage.getItem(REPORT_STATE_SESSION_KEY)
      if (raw) {
        setHandoff(JSON.parse(raw) as ReportStateHandoff)
      }
    } catch {
      // Corrupted sessionStorage — fall through to default grouping.
    } finally {
      setHandoffLoaded(true)
    }
  }, [])

  // Resolve the handoff's cluster itemIds to real items, or fall back
  // to kind-grouping if no handoff was provided.
  const sections: ReportSection[] = useMemo(() => {
    if (!hydrated || !handoffLoaded) return []
    if (handoff && handoff.clusters.length > 0) {
      const resolved = resolveClustersToSections(items, handoff.clusters)
      // Filter out any empty sections (shouldn't happen post-resolve,
      // but defensive)
      const nonEmpty = resolved.filter((s) => s.items.length > 0)
      if (nonEmpty.length > 0) return nonEmpty
    }
    return defaultOrganizationByKind(items)
  }, [hydrated, handoffLoaded, handoff, items])

  const meta: ReportMeta = useMemo(
    () => ({
      title: "Trusight — Bag of Holding Report",
      preparedFor: session?.user?.name ?? session?.user?.email ?? undefined,
      instructions: handoff?.instructions ?? undefined,
      generatedAt: handoff?.generatedAt ?? new Date().toISOString(),
    }),
    [session, handoff],
  )

  const empty = hydrated && handoffLoaded && sections.length === 0

  return (
    <main
      className={cn(
        "report-view",
        "mx-auto max-w-4xl px-6 py-10 space-y-8",
        "print:max-w-none print:px-0 print:py-4",
      )}
    >
      {/* ── Top nav (screen only) ─────────────────────────────────────── */}
      <nav
        aria-label="Report navigation"
        className="flex items-center justify-between gap-4 flex-wrap print:hidden"
      >
        <Link
          href="/collection"
          className={cn(
            "inline-flex items-center gap-2 rounded-md px-3 py-1.5",
            "border border-bronze/60 bg-onyx/60",
            "font-sans text-xs text-ash transition-colors",
            "hover:border-ember hover:bg-iron hover:text-parchment",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember",
          )}
        >
          <ArrowLeft className="h-3.5 w-3.5" aria-hidden />
          <span>Back to Bag of Holding</span>
        </Link>

        {!empty && (
          <ExportButtons sections={sections} meta={meta} />
        )}
      </nav>

      {/* ── Report content ────────────────────────────────────────────── */}
      {!hydrated || !handoffLoaded ? (
        <p className="font-mono text-xs text-ash">Loading report…</p>
      ) : empty ? (
        <EmptyReport />
      ) : (
        <article className="space-y-10">
          <ReportHeader meta={meta} />
          {sections.map((section, idx) => (
            <ReportSectionView key={idx} section={section} />
          ))}
          <ReportFooter />
        </article>
      )}
    </main>
  )
}

// ─── Export button cluster (PDF / Markdown / Plain text) ─────────────────────

function ExportButtons({
  sections,
  meta,
}: {
  sections: ReportSection[]
  meta: ReportMeta
}) {
  function handlePrint() {
    window.print()
  }

  function handleMarkdown() {
    const content = serializeToMarkdown(sections, meta)
    downloadTextFile(
      content,
      defaultFilename("md", meta.generatedAt),
      "text/markdown",
    )
  }

  function handleText() {
    const content = serializeToPlainText(sections, meta)
    downloadTextFile(
      content,
      defaultFilename("txt", meta.generatedAt),
      "text/plain",
    )
  }

  return (
    <div className="flex items-center gap-2 flex-wrap">
      <ExportBtn onClick={handlePrint} icon={<Printer className="h-3.5 w-3.5 text-ember-bright" aria-hidden />}>
        Save as PDF
      </ExportBtn>
      <ExportBtn onClick={handleMarkdown} icon={<FileText className="h-3.5 w-3.5 text-ember-bright" aria-hidden />}>
        Download .md
      </ExportBtn>
      <ExportBtn onClick={handleText} icon={<FileType className="h-3.5 w-3.5 text-ember-bright" aria-hidden />}>
        Download .txt
      </ExportBtn>
    </div>
  )
}

function ExportBtn({
  onClick,
  icon,
  children,
}: {
  onClick: () => void
  icon: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md px-3 py-1.5",
        "border border-bronze/60 bg-onyx/60",
        "font-sans text-xs text-parchment transition-colors",
        "hover:border-ember hover:bg-iron",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian",
      )}
    >
      {icon}
      <span>{children}</span>
    </button>
  )
}

// ─── Report header (title block) ─────────────────────────────────────────────

function ReportHeader({ meta }: { meta: ReportMeta }) {
  const dateStr = new Date(meta.generatedAt ?? Date.now()).toLocaleDateString(
    "en-US",
    { year: "numeric", month: "long", day: "numeric" },
  )

  return (
    <header className="space-y-3 border-b border-bronze/40 pb-6 print:border-b-black/30">
      <p className="font-mono text-[11px] uppercase tracking-[0.24em] text-ember-bright print:text-black">
        Trusight · Bag of Holding Report
      </p>
      <h1 className="font-display text-3xl font-semibold leading-tight text-parchment print:text-black">
        {meta.title}
      </h1>
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1 font-sans text-sm text-ash print:text-neutral-700">
        <span>Generated {dateStr}</span>
        {meta.preparedFor && (
          <>
            <span aria-hidden>·</span>
            <span>Prepared for {meta.preparedFor}</span>
          </>
        )}
      </div>
      {meta.instructions && (
        <blockquote
          className={cn(
            "mt-3 border-l-2 border-ember pl-4 py-1",
            "font-sans text-sm italic text-parchment/85",
            "print:border-l-2 print:border-l-black/60 print:text-neutral-700 print:not-italic",
          )}
        >
          <span className="font-display text-xs not-italic font-semibold text-ember-bright print:text-black block mb-0.5">
            Organization brief
          </span>
          {meta.instructions}
        </blockquote>
      )}
    </header>
  )
}

// ─── Section view ────────────────────────────────────────────────────────────

function ReportSectionView({ section }: { section: ReportSection }) {
  return (
    <section className="space-y-4 print:break-inside-avoid-page">
      <div className="space-y-1.5">
        <h2 className="font-display text-xl font-semibold text-parchment print:text-black">
          {section.title}
        </h2>
        {section.description && (
          <p className="font-sans text-sm text-ash print:text-neutral-700">
            {section.description}
          </p>
        )}
      </div>

      <div className="space-y-4">
        {section.items.map((item) => (
          <ReportItem key={item.id} item={item} />
        ))}
      </div>
    </section>
  )
}

// ─── Item view (renders a single stowed item in report form) ─────────────────

function ReportItem({ item }: { item: StowedItem }) {
  if (item.kind === "card") return <CardItemView item={item} />
  if (item.kind === "sage-answer") return <SageAnswerView item={item} />
  return <SageThreadView item={item} />
}

function CardItemView({ item }: { item: StowedCard }) {
  const subtitleBits = [item.subtitle, item.lens, item.cardType].filter(Boolean)

  return (
    <article
      className={cn(
        "rounded-md border border-bronze/40 bg-onyx/40 p-4",
        "print:border print:border-neutral-300 print:bg-white print:rounded-none",
        "print:break-inside-avoid",
      )}
    >
      <header className="mb-2 flex items-start gap-2">
        <Bookmark
          className="mt-0.5 h-3.5 w-3.5 shrink-0 text-ember-bright print:text-black"
          aria-hidden
        />
        <div className="flex-1 space-y-0.5">
          <h3 className="font-display text-base font-semibold text-parchment print:text-black">
            {item.title}
          </h3>
          {subtitleBits.length > 0 && (
            <p className="font-mono text-[11px] uppercase tracking-widest text-ash/80 print:text-neutral-600">
              {subtitleBits.join(" · ")}
            </p>
          )}
        </div>
      </header>
      <p className="whitespace-pre-wrap font-sans text-sm leading-relaxed text-parchment/90 print:text-neutral-800">
        {item.snapshot}
      </p>
    </article>
  )
}

function SageAnswerView({ item }: { item: StowedSageAnswer }) {
  return (
    <article
      className={cn(
        "rounded-md border border-bronze/40 bg-onyx/40 p-4 space-y-3",
        "print:border print:border-neutral-300 print:bg-white print:rounded-none",
        "print:break-inside-avoid",
      )}
    >
      <header className="flex items-start gap-2">
        <Sparkles
          className="mt-0.5 h-3.5 w-3.5 shrink-0 text-ember-bright print:text-black"
          aria-hidden
        />
        <div className="flex-1">
          <p className="font-display text-sm font-semibold text-parchment print:text-black">
            {item.question}
          </p>
          {item.pageContext && (
            <p className="mt-0.5 font-mono text-[10px] uppercase tracking-widest text-ash/70 print:text-neutral-600">
              Context: {item.pageContext}
            </p>
          )}
        </div>
      </header>
      <p className="whitespace-pre-wrap font-sans text-sm leading-relaxed text-parchment/90 print:text-neutral-800">
        {item.answer}
      </p>
    </article>
  )
}

function SageThreadView({ item }: { item: StowedSageThread }) {
  return (
    <article
      className={cn(
        "rounded-md border border-bronze/40 bg-onyx/40 p-4 space-y-3",
        "print:border print:border-neutral-300 print:bg-white print:rounded-none",
        "print:break-inside-avoid",
      )}
    >
      <header className="flex items-start gap-2">
        <MessageSquare
          className="mt-0.5 h-3.5 w-3.5 shrink-0 text-ember-bright print:text-black"
          aria-hidden
        />
        <div className="flex-1">
          <p className="font-display text-sm font-semibold text-parchment print:text-black">
            Sage conversation
          </p>
          {item.pageContext && (
            <p className="mt-0.5 font-mono text-[10px] uppercase tracking-widest text-ash/70 print:text-neutral-600">
              Context: {item.pageContext}
            </p>
          )}
        </div>
      </header>
      <div className="space-y-2.5">
        {item.messages.map((m, idx) => (
          <div key={idx} className="space-y-0.5">
            <p className="font-mono text-[10px] uppercase tracking-widest text-ember-bright print:text-neutral-600">
              {m.role === "user" ? "User" : "Sage"}
            </p>
            <p className="whitespace-pre-wrap font-sans text-sm text-parchment/90 print:text-neutral-800">
              {m.text}
            </p>
          </div>
        ))}
      </div>
    </article>
  )
}

// ─── Footer (screen only — hidden from print to avoid redundancy with
// the browser's auto-added URL chrome, and to give the PDF a cleaner
// end). Onscreen the footer acts as a visual "report ends here" marker. ─────

function ReportFooter() {
  return (
    <footer className="border-t border-bronze/40 pt-4 print:hidden">
      <p className="font-mono text-[10px] uppercase tracking-widest text-ash/70">
        Generated by Trusight · trusightdata.ai
      </p>
    </footer>
  )
}

function EmptyReport() {
  return (
    <div className="rounded-lg border border-bronze/40 bg-onyx/40 p-8 text-center">
      <p className="font-display text-base text-parchment">Your Bag is empty.</p>
      <p className="mt-2 font-sans text-sm text-ash">
        Stow some cards or Sage answers first, then come back here to organize
        them into a report.
      </p>
      <Link
        href="/collection"
        className="mt-4 inline-block font-sans text-sm text-ember-bright underline underline-offset-4 hover:text-ember"
      >
        Back to Bag of Holding
      </Link>
    </div>
  )
}
