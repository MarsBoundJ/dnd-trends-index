"use client"

/*
 * Arcane Analytics — Generate Report form.
 *
 * The control surface at the top of /collection that turns a Bag of
 * Holding into an organized, exportable report. User optionally types
 * organization instructions ("group by IP licensing potential", "separate
 * digital from tabletop") and clicks Generate; Sage clusters the items
 * via /api/bag/organize; the response is stashed in sessionStorage and
 * the browser navigates to /collection/report.
 *
 * Empty-input path: still hits Sage (default thematic clustering).
 * Fallback path if Sage errors: the report page gracefully falls back
 * to a content-kind grouping (cards / sage-answers / sage-threads) so
 * the user never gets stuck.
 *
 * The text input has a 280-char limit — generous enough for real
 * framing instructions, tight enough to prevent absurd prompts.
 */

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Wand2, Loader2 } from "lucide-react"

import { useBagStore, type StowedItem } from "@/lib/bag-store"
import { cn } from "@/lib/utils"

const MAX_INSTRUCTIONS_LENGTH = 280

/**
 * Session-storage key the report page reads to find the organization
 * Sage just produced. Ephemeral by design — a fresh Generate click
 * always overwrites.
 */
export const REPORT_STATE_SESSION_KEY = "arcane:report-state:v1"

/**
 * Shape of the payload handed off to /collection/report via sessionStorage.
 * The report page resolves cluster itemIds against the live Zustand bag
 * so we don't duplicate item data here.
 */
export interface ReportStateHandoff {
  instructions: string | null
  clusters: Array<{ title: string; description?: string; itemIds: string[] }>
  generatedAt: string
}

export function GenerateReportForm() {
  const router = useRouter()
  const items = useBagStore((s) => s.items)
  const [instructions, setInstructions] = useState("")
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const disabled = items.length === 0 || isSubmitting

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (disabled) return

    setError(null)
    setIsSubmitting(true)

    try {
      // Build the slim payload — only what Sage needs for grouping.
      // Full item content stays in the Zustand bag; we don't round-trip it.
      const payloadItems = items.map((item) => ({
        id: item.id,
        kind: item.kind,
        title: getItemTitleForPayload(item),
        excerpt: getItemExcerptForPayload(item),
      }))

      const response = await fetch("/api/bag/organize", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: payloadItems,
          instructions: instructions.trim() || undefined,
        }),
      })

      if (!response.ok) {
        const errBody = await response.json().catch(() => ({}))
        throw new Error(
          errBody.error ?? `Sage couldn't organize the bag (HTTP ${response.status}).`,
        )
      }

      const result = await response.json() as {
        sections: Array<{ title: string; description?: string; itemIds: string[] }>
      }

      // Stash the cluster organization for the report page to pick up.
      const handoff: ReportStateHandoff = {
        instructions: instructions.trim() || null,
        clusters: result.sections,
        generatedAt: new Date().toISOString(),
      }
      sessionStorage.setItem(REPORT_STATE_SESSION_KEY, JSON.stringify(handoff))

      router.push("/collection/report")
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Something went wrong generating the report.",
      )
      setIsSubmitting(false)
    }
  }

  const charsRemaining = MAX_INSTRUCTIONS_LENGTH - instructions.length
  const nearLimit = charsRemaining <= 40

  return (
    <form
      onSubmit={handleSubmit}
      className={cn(
        "rounded-lg border border-bronze/60 bg-onyx/40 p-5",
        "space-y-4",
      )}
    >
      <div className="space-y-2">
        <label
          htmlFor="report-instructions"
          className="font-display text-sm font-semibold text-parchment"
        >
          Tell Sage how to organize your Bag of Holding{" "}
          <span className="text-ash font-sans font-normal">(optional)</span>
        </label>
        <textarea
          id="report-instructions"
          value={instructions}
          onChange={(e) => setInstructions(e.target.value.slice(0, MAX_INSTRUCTIONS_LENGTH))}
          placeholder="e.g., &ldquo;group by IP licensing potential&rdquo;, &ldquo;separate digital from tabletop&rdquo;, &ldquo;cluster by theme&rdquo;"
          rows={2}
          className={cn(
            "w-full rounded-md border border-bronze/40 bg-obsidian/60",
            "px-3 py-2 font-sans text-sm text-parchment",
            "placeholder:text-ash/60",
            "focus:outline-none focus:border-ember focus:ring-2 focus:ring-ember/30",
            "resize-none",
          )}
          disabled={isSubmitting}
          maxLength={MAX_INSTRUCTIONS_LENGTH}
        />
        <div className="flex items-center justify-between">
          <p className="font-sans text-xs text-ash/70">
            Or just hit Generate — Sage will pick a default grouping.
          </p>
          <p
            className={cn(
              "font-mono text-[10px]",
              nearLimit ? "text-ember-bright" : "text-ash/50",
            )}
          >
            {instructions.length}/{MAX_INSTRUCTIONS_LENGTH}
          </p>
        </div>
      </div>

      {error && (
        <p
          role="alert"
          className="rounded-md border border-rarity-copper/60 bg-rarity-copper/10 px-3 py-2 font-sans text-xs text-rarity-copper"
        >
          {error}
        </p>
      )}

      <button
        type="submit"
        disabled={disabled}
        className={cn(
          "inline-flex items-center gap-2 rounded-md px-4 py-2",
          "border border-ember bg-ember/15",
          "font-sans text-sm font-medium text-parchment",
          "transition-colors hover:bg-ember/25 hover:border-ember-bright",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian",
          "disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-ember/15 disabled:hover:border-ember",
        )}
      >
        {isSubmitting ? (
          <>
            <Loader2 className="h-4 w-4 animate-spin text-ember-bright" aria-hidden />
            <span>Sage is organizing&hellip;</span>
          </>
        ) : (
          <>
            <Wand2 className="h-4 w-4 text-ember-bright" aria-hidden />
            <span>Generate Report</span>
          </>
        )}
      </button>
    </form>
  )
}

// ─── Payload helpers (keep the organize endpoint's prompt lean) ──────────────

function getItemTitleForPayload(item: StowedItem): string {
  if (item.kind === "card") return item.title
  if (item.kind === "sage-answer") return item.question
  // sage-thread
  const firstUser = item.messages.find((m) => m.role === "user")
  return firstUser?.text ?? "Untitled Sage thread"
}

function getItemExcerptForPayload(item: StowedItem): string {
  if (item.kind === "card") return item.snapshot
  if (item.kind === "sage-answer") return item.answer
  // sage-thread — first assistant response is usually the meat
  const firstAssistant = item.messages.find((m) => m.role === "assistant")
  return firstAssistant?.text ?? ""
}
