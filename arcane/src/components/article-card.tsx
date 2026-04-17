"use client"

/*
 * Arcane Analytics — ArticleCard (Step 9b).
 *
 * Renders a single Council article row from `gold_data.daily_articles`
 * inside the universal CardChrome shell. Each Council member has a
 * fixed Lucide sigil; the byline row shows sigil + name + beat, with
 * the one-line bio behind a click/tap Popover.
 *
 * Confidence is stubbed at 75 (silver) — same pattern Step 3 used before
 * the Step 6 formula landed for concept-level scores. Article-level
 * confidence is a follow-up (there's no concept to look up against
 * `gold_data.concept_confidence` here).
 */

import ReactMarkdown from "react-markdown"
import {
  Scroll,
  Crown,
  Anchor,
  Spline,
  Compass,
  Quote,
  type LucideIcon,
} from "lucide-react"

import { CardChrome } from "@/components/card-chrome"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { cn } from "@/lib/utils"
import type { Article } from "@/lib/bouncer"

// Confirmed sigil mapping from the Step 9b brief (2026-04-17).
// Keyed by author_name exactly as written to BigQuery in council.py.
// Unknown names fall back to `Quote` so a legacy or mis-spelled byline
// still renders rather than crashing.
const sigilByAuthor: Record<string, LucideIcon> = {
  "The Loremaster": Scroll,
  "The Bursar": Crown,
  "The Quartermaster": Anchor,
  "The Weaver": Spline,
  "The Architect": Compass,
}

// STUB — confidence scoring for articles lands in a follow-up.
// Same pattern Step 3 used pre-Step-6. Once article-level confidence
// is defined (probably weighted average of the underlying data sources
// the Council member cited), this becomes a real computation.
const STUB_CONFIDENCE = 75

function formatDate(isoDate: string): string {
  // Input format from BigQuery is "YYYY-MM-DD". Render as the US long form
  // without timezone drift (`new Date("2026-04-17")` would get treated as
  // UTC midnight and render as Apr 16 in America/Chicago).
  const [y, m, d] = isoDate.split("-").map(Number)
  if (!y || !m || !d) return isoDate
  const date = new Date(Date.UTC(y, m - 1, d))
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  })
}

export function ArticleCard({ article }: { article: Article }) {
  const Sigil = sigilByAuthor[article.author_name] ?? Quote

  return (
    <CardChrome
      title={article.headline}
      subtitle={formatDate(article.date)}
      titleVariant="prominent"
      cardType="article"
      confidence={STUB_CONFIDENCE}
      cardId={`article:${article.date}:${article.author_name}`}
      sageContext={[
        `Article — ${article.headline}`,
        `By ${article.author_name} · ${article.author_beat}`,
        `Date: ${article.date}`,
        `Hook: ${article.hook}`,
        `Key stat: ${article.key_stat}`,
        "",
        article.body_markdown,
      ].join("\n")}
    >
      <div className="flex flex-col gap-3 pt-1">
        {/* ── Byline row ─────────────────────────────────────────────── */}
        <Popover>
          <PopoverTrigger asChild>
            <button
              type="button"
              aria-label={`About ${article.author_name}`}
              className={cn(
                "group flex items-start gap-2 -ml-1 pl-1 py-1 rounded-md",
                "text-left transition-colors",
                "hover:bg-iron focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-onyx",
              )}
            >
              <Sigil
                className="h-4 w-4 mt-0.5 shrink-0 text-ember-bright"
                aria-hidden
              />
              <span className="flex flex-col leading-tight">
                <span className="font-display text-sm font-medium text-parchment">
                  {article.author_name}
                </span>
                <span className="font-sans text-[11px] text-ash">
                  {article.author_beat}
                </span>
              </span>
            </button>
          </PopoverTrigger>
          <PopoverContent
            side="bottom"
            align="start"
            className="w-72 bg-iron border border-bronze text-parchment p-3 space-y-2"
          >
            <div className="flex items-center gap-2">
              <Sigil className="h-4 w-4 text-ember-bright" aria-hidden />
              <p className="font-display text-sm font-semibold">
                {article.author_name}
              </p>
            </div>
            <p className="font-mono uppercase tracking-widest text-[10px] text-ember">
              {article.author_beat}
            </p>
            <p className="text-xs text-parchment/85 leading-relaxed">
              {article.author_bio}
            </p>
          </PopoverContent>
        </Popover>

        {/* ── Hook ───────────────────────────────────────────────────── */}
        <p className="font-display text-[15px] italic text-parchment/90 leading-snug border-l-2 border-ember/60 pl-3">
          {article.hook}
        </p>

        {/* ── Body ───────────────────────────────────────────────────── */}
        <div
          className={cn(
            "prose-article text-sm text-parchment/90 leading-relaxed",
            "[&_p]:mb-3 [&_p:last-child]:mb-0",
            "[&_h2]:font-display [&_h2]:text-base [&_h2]:font-semibold [&_h2]:text-parchment [&_h2]:mt-4 [&_h2]:mb-2",
            "[&_h3]:font-display [&_h3]:text-sm [&_h3]:font-semibold [&_h3]:text-parchment [&_h3]:mt-3 [&_h3]:mb-1.5",
            "[&_ul]:list-disc [&_ul]:pl-5 [&_ul]:mb-3 [&_ol]:list-decimal [&_ol]:pl-5 [&_ol]:mb-3",
            "[&_li]:mb-1",
            "[&_strong]:text-parchment [&_strong]:font-semibold",
            "[&_em]:text-parchment/80",
            "[&_code]:font-mono [&_code]:text-xs [&_code]:bg-iron [&_code]:px-1 [&_code]:py-0.5 [&_code]:rounded",
          )}
        >
          <ReactMarkdown>{article.body_markdown}</ReactMarkdown>
        </div>

        {/* ── Key stat chip ──────────────────────────────────────────── */}
        {article.key_stat && (
          <div className="flex items-center gap-2 pt-1">
            <span className="font-mono uppercase tracking-widest text-[10px] text-ash/70">
              Key stat
            </span>
            <span className="font-mono text-xs text-ember-bright bg-iron border border-bronze/60 rounded px-2 py-0.5">
              {article.key_stat}
            </span>
          </div>
        )}
      </div>
    </CardChrome>
  )
}
