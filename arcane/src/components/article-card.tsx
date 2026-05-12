"use client"

/*
 * Trusight — ArticleCard (Step 9b).
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
  Telescope,
  Dices,
  GraduationCap,
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
import {
  ARTICLE_DISCLAIMER,
  COUNCIL_PERSONA_DISCLAIMER,
} from "@/lib/disclaimer"

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
  "The Chronicler": Telescope,
  // Gamer Gary breaks the "The X" convention on purpose — first-name
  // handle format parallels Sage (also name-based) and signals the
  // perspective-not-expertise distinction for our two demand-side
  // voices. Voice character: Ginny Di (warmth, social contract) +
  // Sly Flourish (pragmatic-protector Lazy DM) + the "r/DnDBehindthe
  // Screen regular whose 8-paragraph post everyone upvotes."
  "Gamer Gary": Dices,
  // The Dean lands as the 8th Council voice in Step 9.10 — academic-
  // observer register built from Thompson (Stratechery) + Galloway
  // (No Mercy / NYU Stern) + HBR/McKinsey composite (carrying
  // Christensen + Kim). Sigil GraduationCap fits the expertise-title
  // pattern. A hand-crafted reference article ("The D&D Book Isn't
  // Dying. It's Moving Warehouses.") landed Apr 21 as a smoke-test
  // acceptance criterion; the real voice-block synthesis ships in
  // Step 9.10 proper.
  "The Dean": GraduationCap,
}

// STUB — confidence scoring for articles lands in a follow-up.
// Same pattern Step 3 used pre-Step-6. Once article-level confidence
// is defined (probably weighted average of the underlying data sources
// the Council member cited), this becomes a real computation.
// Co-author weighting (Step 9.8b) will replace this with a
// multi-author aggregate.
const STUB_CONFIDENCE = 75

// Step 9.8 — produce "The Bursar & The Weaver" or
// "The Bursar, The Weaver & The Quartermaster" from primary + co-authors.
// Single-author bylines return the primary name unchanged.
function formatByline(primary: string, coAuthors: readonly string[]): string {
  if (coAuthors.length === 0) return primary
  if (coAuthors.length === 1) return `${primary} & ${coAuthors[0]}`
  const init = coAuthors.slice(0, -1).join(", ")
  const last = coAuthors[coAuthors.length - 1]
  return `${primary}, ${init} & ${last}`
}

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

export function ArticleCard({
  article,
  frameLabel,
}: {
  article: Article
  /**
   * Step 9.8 — human label of the interpretive frame the article was
   * written through, e.g. "Hasbro / Wizards of the Coast — Playing to
   * Win (FY26)". Looked up server-side from the frame doc so the UI
   * adapts automatically when an admin swaps frames (paizo-2026,
   * industry-fundamentals). Rendered only on Track D articles.
   */
  frameLabel?: string | null
}) {
  const Sigil = sigilByAuthor[article.author_name] ?? Quote
  const isFlash = article.length === "flash"

  // Step 9.8 — co-bylines. Empty array on single-author rows (most
  // articles). When present, render secondary sigils adjacent to the
  // primary and extend the byline text. Popover stays on the primary
  // only for 9.8 scope — multi-popover UX ships with 9.8b when the
  // journalist actually emits co-authored prose.
  const coAuthors = article.co_authors ?? []
  const bylineName = formatByline(article.author_name, coAuthors)
  const coAuthorSigils = coAuthors.map((name) => sigilByAuthor[name] ?? Quote)

  // Step 9.8 + 9.10 — frame attribution. Track D (corporate-strategy)
  // and Track C (industry-fundamentals academic lens) both render the
  // visible "Through the {frame}:" label so readers know which
  // interpretive prior is applied. Tracks A + B don't — A is data-
  // native (no frame), B uses own-expertise (no interpretive overlay
  // worth surfacing).
  const showFrameAttribution =
    (article.track === "D" || article.track === "C") &&
    !!frameLabel &&
    !!article.frame_id

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
        `By ${bylineName} · ${article.author_beat}`,
        `Date: ${article.date}`,
        showFrameAttribution ? `Through the frame: ${frameLabel}` : null,
        `Hook: ${article.hook}`,
        `Key stat: ${article.key_stat}`,
        "",
        article.body_markdown,
      ].filter(Boolean).join("\n")}
    >
      <div className={cn("flex flex-col pt-1", isFlash ? "gap-2" : "gap-3")}>
        {showFrameAttribution && (
          <p
            className="font-mono uppercase tracking-widest text-[10px] text-ember-bright/80 -mb-1"
            aria-label={`Interpretive frame: ${frameLabel}`}
          >
            Through the {frameLabel}
          </p>
        )}
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
              <span className="flex items-center gap-1 mt-0.5 shrink-0">
                <Sigil
                  className="h-4 w-4 text-ember-bright"
                  aria-hidden
                />
                {coAuthorSigils.map((CoSigil, i) => (
                  // Secondary sigils render slightly dimmed so the
                  // visual hierarchy reads "primary + co-bylines" at a
                  // glance. Hover/focus affordances stay on the primary
                  // popover for Step 9.8.
                  <CoSigil
                    key={`${coAuthors[i]}-${i}`}
                    className="h-4 w-4 text-ember-bright/70"
                    aria-hidden
                  />
                ))}
              </span>
              <span className="flex flex-col leading-tight">
                <span className="font-display text-sm font-medium text-parchment">
                  {bylineName}
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
            {/* Council persona disclaimer — clarifies the voice is a
                synthetic composite, not a real analyst. Protects
                against leaked quotes being misread as from a real
                person (e.g., "Chris Cocks said X" when actually the
                Bursar voice — which draws on Cocks + Härenstam +
                Stevens/Butler — said X). */}
            <p className="font-mono text-[10px] leading-snug text-ash/60 pt-2 mt-1 border-t border-bronze/20">
              {COUNCIL_PERSONA_DISCLAIMER}
            </p>
          </PopoverContent>
        </Popover>

        {/* ── Hook ───────────────────────────────────────────────────── */}
        {/* Flash cards render the hook one visual notch tighter — the card
            IS the article, so every gram of whitespace counts. */}
        <p
          className={cn(
            "font-display italic text-parchment/90 leading-snug border-l-2 border-ember/60 pl-3",
            isFlash ? "text-[14px]" : "text-[15px]",
          )}
        >
          {article.hook}
        </p>

        {/* ── Body (Standard + Report only; Flash has empty body_markdown by
             design — the whole point of Flash is that the hook IS the
             article). */}
        {!isFlash && article.body_markdown && (
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
        )}

        {/* ── Key stat chip ──────────────────────────────────────────── */}
        {/* Flash drops the "Key stat" label — the entire card is the
             stat already, so the label is redundant throat-clearing. */}
        {article.key_stat && (
          <div className="flex items-center gap-2 pt-1">
            {!isFlash && (
              <span className="font-mono uppercase tracking-widest text-[10px] text-ash/70">
                Key stat
              </span>
            )}
            <span className="font-mono text-xs text-ember-bright bg-iron border border-bronze/60 rounded px-2 py-0.5">
              {article.key_stat}
            </span>
          </div>
        )}

        {/* ── AI disclaimer ──────────────────────────────────────────── */}
        {/* Inside the card (not outside) so it travels with every
             screenshot. Mono + small + low-opacity = "fine print"
             register. Subordinate visual weight; present on every
             length variant including Flash. */}
        <p
          className={cn(
            "font-mono text-[10px] leading-snug text-ash/60",
            "pt-3 mt-1 border-t border-bronze/20",
          )}
        >
          {ARTICLE_DISCLAIMER}
        </p>
      </div>
    </CardChrome>
  )
}
