"use client"

/*
 * Arcane Analytics — Sage panel (Steps 4 + 6.5 + 7).
 *
 * Floating right-side chat panel. Talks to /api/sage via the Vercel AI SDK
 * `useChat` hook. Page context (a plain-text snapshot of the current page or
 * card) is passed on every send as an extra body field and injected into the
 * system prompt by the route handler.
 *
 * Step 4: Visual panel + streaming chat.
 * Step 6.5: Post-stream grounding check with inline footnotes.
 * Step 7: Tool invocation rendering — compact chips show which tools
 *         the Sage calls (getLeaderboard, searchConcepts, etc.) and their
 *         state (loading/done/error). Tool results are included in the
 *         grounding context so tool-sourced claims are properly scored.
 *
 * The panel is driven by a <SageProvider> higher in the tree that exposes
 * `openSage(ctx)` / `closeSage()` via React context, so any CardChrome's
 * onExplain handler can pop it open with that card's data as context.
 */

import React from "react"
import { useChat } from "@ai-sdk/react"
import { DefaultChatTransport } from "ai"
import { Sparkles, X, Bookmark, Shield, ShieldCheck, ShieldAlert, Database } from "lucide-react"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import { useBagStore, useBagHasHydrated } from "@/lib/bag-store"
import {
  type GroundingResult,
  type GroundingCitation,
  positionedCitations,
  groundingTier,
} from "@/lib/grounding"

// ─── Tool display names ──────────────────────────────────────────────────────
// Human-readable labels for each tool. Shown in the chat when the Sage
// calls a tool so the user knows what's happening behind the scenes.

const TOOL_LABELS: Record<string, string> = {
  getLeaderboard: "Leaderboard data",
  searchConcepts: "Concept library",
  getConceptConfidence: "Confidence scores",
  getMarketSummary: "Market summary",
  getTopMovers: "Top movers",
  getDmShortageIndex: "DM shortage data",
  getEditionMigration: "Edition migration",
  getSystemHealth: "System health",
}

// ─── Sage context ────────────────────────────────────────────────────────────

interface SageContextValue {
  isOpen: boolean
  pageContext: string
  openSage: (pageContext: string) => void
  closeSage: () => void
}

const SageContext = React.createContext<SageContextValue | null>(null)

export function useSage() {
  const ctx = React.useContext(SageContext)
  if (!ctx) throw new Error("useSage must be used inside <SageProvider>")
  return ctx
}

/**
 * Like `useSage`, but returns `null` when no `<SageProvider>` is mounted.
 * Useful for components that live in multiple contexts (harness pages,
 * tests) and should gracefully no-op when Sage isn't available.
 */
export function useSageOptional() {
  return React.useContext(SageContext)
}

export function SageProvider({ children }: { children: React.ReactNode }) {
  const [isOpen, setIsOpen] = React.useState(false)
  const [pageContext, setPageContext] = React.useState("")

  const openSage = React.useCallback((ctx: string) => {
    setPageContext(ctx)
    setIsOpen(true)
  }, [])

  const closeSage = React.useCallback(() => setIsOpen(false), [])

  const value = React.useMemo(
    () => ({ isOpen, pageContext, openSage, closeSage }),
    [isOpen, pageContext, openSage, closeSage]
  )

  return (
    <SageContext.Provider value={value}>
      {children}
      <SagePanel />
    </SageContext.Provider>
  )
}

// ─── Sage panel ──────────────────────────────────────────────────────────────

function SagePanel() {
  const { isOpen, pageContext, closeSage } = useSage()
  const [input, setInput] = React.useState("")
  const scrollRef = React.useRef<HTMLDivElement>(null)

  // Transport is constant across renders — any per-request data (like the
  // current pageContext) is attached at sendMessage time via request-level
  // body options. The useChat docs flag request-level options as the
  // recommended pattern over hook-level dynamic config.
  const transport = React.useMemo(
    () => new DefaultChatTransport({ api: "/api/sage" }),
    []
  )

  const { messages, sendMessage, status, stop, error } = useChat({
    transport,
  })

  // ── Bag of Holding wiring (Step 5 §3.5) ─────────────────────────────────
  // Sage answers and whole conversations are stowable alongside data cards.
  // Subscribing to `items` keeps the Stow buttons reactive — clicking a
  // button flips its label immediately without a re-fetch.
  //
  // NOTE: The selector must return a stable reference. `s.items.map(...)`
  // would build a fresh array on every call and break Zustand v5's
  // `useSyncExternalStore` snapshot check ("getServerSnapshot should be
  // cached to avoid an infinite loop"). Select `items` itself — same
  // identity unless the array actually changes — and derive downstream.
  const bagHydrated = useBagHasHydrated()
  const bagItems = useBagStore((s) => s.items)
  const stowSageAnswer = useBagStore((s) => s.stowSageAnswer)
  const stowSageThread = useBagStore((s) => s.stowSageThread)
  const unstow = useBagStore((s) => s.unstow)
  const isItemStowed = (id: string) => bagItems.some((i) => i.id === id)

  // ── Grounding check state (Step 6.5) ─────────────────────────────────────
  // Maps assistant message IDs to their grounding results. The check fires
  // once per message after streaming finishes; results persist for the
  // lifetime of the panel.
  const [groundingResults, setGroundingResults] = React.useState<
    Record<string, GroundingResult>
  >({})
  const [groundingInProgress, setGroundingInProgress] = React.useState<
    Set<string>
  >(new Set())
  // Track which message ID was the last one we checked, so we don't re-fire
  const lastGroundedRef = React.useRef<string | null>(null)

  // Trigger grounding check when the latest assistant message finishes streaming.
  React.useEffect(() => {
    // Only trigger when status transitions to ready (not streaming, not submitted)
    if (status !== "ready") return
    if (messages.length === 0) return

    const lastMsg = messages[messages.length - 1]
    if (lastMsg.role !== "assistant") return
    if (lastMsg.id === lastGroundedRef.current) return
    if (groundingResults[lastMsg.id]) return
    if (groundingInProgress.has(lastMsg.id)) return

    const text = messageText(lastMsg)
    if (!text.trim()) return

    // Mark as in-progress and fire the check
    lastGroundedRef.current = lastMsg.id
    setGroundingInProgress((prev) => new Set(prev).add(lastMsg.id))

    // Include tool results in the grounding context so tool-sourced claims
    // aren't flagged as ungrounded. The grounding model sees both the passive
    // pageContext AND the active tool results as "source data."
    const toolContext = extractToolContext(lastMsg)
    const fullContext = pageContext + toolContext

    fetch("/api/sage/ground", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        generatedText: text,
        pageContext: fullContext,
      }),
    })
      .then((res) => res.json())
      .then((result: GroundingResult) => {
        setGroundingResults((prev) => ({ ...prev, [lastMsg.id]: result }))
        setGroundingInProgress((prev) => {
          const next = new Set(prev)
          next.delete(lastMsg.id)
          return next
        })
      })
      .catch((err) => {
        console.error("[sage-panel] Grounding check failed:", err)
        setGroundingInProgress((prev) => {
          const next = new Set(prev)
          next.delete(lastMsg.id)
          return next
        })
      })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status, messages])

  // Auto-scroll to the newest message as tokens stream in.
  React.useEffect(() => {
    if (!scrollRef.current) return
    scrollRef.current.scrollTop = scrollRef.current.scrollHeight
  }, [messages, status, groundingResults])

  if (!isOpen) return null

  const isBusy = status === "submitted" || status === "streaming"

  // ── Helpers ─────────────────────────────────────────────────────────────
  // Flatten a UIMessage's parts array down to plain text for storage.
  // Tool invocation parts are skipped — only the model's text summary
  // of tool results is captured. This is intentional: stowed text should
  // be the human-readable narrative, not raw JSON tool outputs.
  const messageText = (m: (typeof messages)[number]): string =>
    m.parts
      .filter((p): p is Extract<typeof p, { type: "text" }> => p.type === "text")
      .map((p) => p.text)
      .join("")

  // Extract tool result data from a message for grounding context.
  // When the Sage calls tools, the grounding check needs to know what
  // data was available — otherwise it flags tool-sourced claims as
  // ungrounded because they're not in pageContext.
  const extractToolContext = (m: (typeof messages)[number]): string => {
    const toolParts = m.parts.filter(
      (p) => p.type === "dynamic-tool" || p.type.startsWith("tool-")
    )
    if (toolParts.length === 0) return ""

    const summaries = toolParts
      .map((p) => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const part = p as any
        const name = part.toolName ?? part.type.replace("tool-", "")
        if (part.state !== "output-available" || !part.output) return null
        return `Tool "${name}" returned:\n${JSON.stringify(part.output, null, 2)}`
      })
      .filter(Boolean)

    return summaries.length > 0
      ? "\n\n## Tool results (data the AI retrieved via live queries):\n\n" +
          summaries.join("\n\n")
      : ""
  }

  // Given an assistant message id, find the most recent preceding user
  // message — that's the Q in the Q+A pair we're about to stow.
  const findPreviousUserMessage = (assistantId: string): string => {
    const idx = messages.findIndex((m) => m.id === assistantId)
    if (idx === -1) return ""
    for (let i = idx - 1; i >= 0; i--) {
      if (messages[i].role === "user") return messageText(messages[i])
    }
    return ""
  }

  const stowAnswerId = (assistantMessageId: string) =>
    `sage:answer:${assistantMessageId}`

  const handleStowAnswer = (assistantMessageId: string) => {
    const id = stowAnswerId(assistantMessageId)
    if (isItemStowed(id)) {
      unstow(id)
      return
    }
    const msg = messages.find((m) => m.id === assistantMessageId)
    if (!msg) return
    stowSageAnswer({
      id,
      pageContext,
      question: findPreviousUserMessage(assistantMessageId),
      answer: messageText(msg),
    })
  }

  const handleStowThread = () => {
    if (messages.length === 0) return
    // Each stow of a thread gets a fresh id so users can stow progressive
    // snapshots of the same conversation as it grows (e.g. "stow me at the
    // halfway point, then again at the end").
    stowSageThread({
      id: `sage:thread:${Date.now()}`,
      pageContext,
      messages: messages.map((m) => ({
        role: m.role === "user" ? "user" : "assistant",
        text: messageText(m),
      })),
    })
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || isBusy) return
    // Attach the current pageContext at send time so the route handler can
    // inject it into the system prompt. Latest value always wins even if
    // the user opens Sage from a different card mid-conversation.
    sendMessage({ text: input }, { body: { pageContext } })
    setInput("")
  }

  return (
    <aside
      role="dialog"
      aria-label="Sage — D&D trend oracle"
      className={cn(
        "fixed right-0 top-0 z-50 flex h-screen w-full max-w-md flex-col",
        "border-l border-bronze bg-onyx text-parchment shadow-2xl"
      )}
    >
      {/* Header */}
      <header className="flex items-start gap-3 border-b border-bronze px-5 py-4">
        <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-ember/60 bg-iron">
          <Sparkles className="h-4 w-4 text-ember-bright" />
        </div>
        <div className="flex-1 min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-widest text-ember-bright">
            The Sage · Strategist
          </p>
          <h2 className="font-display text-base font-semibold leading-tight text-parchment">
            Ask about what you see
          </h2>
          {pageContext && (
            <p className="mt-1 truncate font-mono text-[10px] text-ash/70">
              context: {pageContext.split("\n")[0]}
            </p>
          )}
        </div>
        <div className="flex items-center gap-1">
          {/* Stow the whole conversation as a read-only artifact. Disabled
              until at least one assistant response has arrived and the bag
              store has hydrated from localStorage. */}
          <Button
            variant="ghost"
            size="sm"
            onClick={handleStowThread}
            disabled={!bagHydrated || messages.length === 0 || isBusy}
            aria-label="Stow this conversation in the Bag of Holding"
            title="Stow this conversation"
            className="h-7 w-7 p-0 text-ash hover:bg-iron hover:text-ember-bright"
          >
            <Bookmark className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={closeSage}
            aria-label="Close Sage"
            className="h-7 w-7 p-0 text-ash hover:bg-iron hover:text-parchment"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </header>

      {/* Message list */}
      <div
        ref={scrollRef}
        className="flex-1 space-y-4 overflow-y-auto px-5 py-5"
      >
        {messages.length === 0 && (
          <p className="font-sans text-sm text-ash">
            The Sage is watching the same page you are. Ask what&rsquo;s moving,
            why, or what to do about it.
          </p>
        )}

        {messages.map((m) => {
          const isAssistant = m.role === "assistant"
          const answerId = stowAnswerId(m.id)
          const isAnswerStowed = bagHydrated && isItemStowed(answerId)
          // Don't let the user stow an assistant message while it's still
          // streaming — the text isn't final yet.
          const canStowAnswer =
            isAssistant && !(isBusy && m.id === messages[messages.length - 1].id)

          // Grounding results for this assistant message (Step 6.5)
          const grounding = isAssistant ? groundingResults[m.id] : undefined
          const isGrounding = isAssistant && groundingInProgress.has(m.id)
          const text = messageText(m)
          const positioned = grounding
            ? positionedCitations(text, grounding)
            : []

          return (
            <div
              key={m.id}
              className={cn(
                "flex flex-col gap-1",
                m.role === "user" ? "items-end" : "items-start"
              )}
            >
              <span className="font-mono text-[10px] uppercase tracking-widest text-ash/70">
                {m.role === "user" ? "You" : "Sage"}
              </span>
              <div
                className={cn(
                  "max-w-[90%] rounded-lg border px-3 py-2 font-sans text-sm leading-relaxed whitespace-pre-wrap",
                  m.role === "user"
                    ? "border-bronze bg-iron text-parchment"
                    : "border-ember/40 bg-onyx text-parchment"
                )}
              >
                {/* Tool invocation chips — always rendered above text */}
                {isAssistant &&
                  m.parts
                    .filter(
                      (p) =>
                        p.type === "dynamic-tool" ||
                        p.type.startsWith("tool-")
                    )
                    .map((part, i) => (
                      <ToolInvocationChip key={`tool-${i}`} part={part} />
                    ))}
                {/* Render text with inline footnote markers if grounded */}
                {isAssistant && positioned.length > 0
                  ? renderAnnotatedText(text, positioned)
                  : m.parts.map((part, i) =>
                      part.type === "text" ? (
                        <span key={i}>{part.text}</span>
                      ) : null
                    )}
              </div>

              {/* Grounding byline — shows after grounding check completes */}
              {isAssistant && grounding && (
                <GroundingByline grounding={grounding} />
              )}

              {/* Grounding footnotes — the detailed breakdown */}
              {isAssistant && positioned.length > 0 && grounding && (
                <GroundingFootnotes citations={positioned} />
              )}

              {/* "Verifying claims..." indicator while grounding runs */}
              {isGrounding && (
                <span className="flex items-center gap-1.5 mt-0.5 font-mono text-[10px] uppercase tracking-widest text-ash/50">
                  <Shield className="h-3 w-3 animate-pulse" />
                  Verifying claims…
                </span>
              )}

              {isAssistant && canStowAnswer && (
                <button
                  type="button"
                  onClick={() => handleStowAnswer(m.id)}
                  aria-label={
                    isAnswerStowed
                      ? "Remove this answer from the Bag of Holding"
                      : "Stow this answer in the Bag of Holding"
                  }
                  aria-pressed={isAnswerStowed}
                  className={cn(
                    "mt-0.5 flex items-center gap-1 font-mono text-[10px] uppercase tracking-widest transition-colors",
                    "focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ember focus-visible:ring-offset-1 focus-visible:ring-offset-onyx rounded-sm",
                    isAnswerStowed
                      ? "text-ember-bright"
                      : "text-ash/60 hover:text-ember-bright"
                  )}
                >
                  <Bookmark
                    className={cn(
                      "h-3 w-3",
                      isAnswerStowed && "fill-current"
                    )}
                  />
                  {isAnswerStowed ? "Stowed" : "Stow answer"}
                </button>
              )}
            </div>
          )
        })}

        {status === "submitted" && (
          <p className="font-mono text-[10px] uppercase tracking-widest text-ash/70">
            Sage is thinking…
          </p>
        )}

        {error && (
          <p className="font-mono text-xs text-rarity-copper">
            Sage failed to respond. Check Vertex AI credentials and try again.
          </p>
        )}
      </div>

      {/* Composer */}
      <form
        onSubmit={handleSubmit}
        className="border-t border-bronze px-5 py-4"
      >
        <div className="flex items-end gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault()
                handleSubmit(e)
              }
            }}
            rows={2}
            placeholder="Ask the Sage…"
            disabled={isBusy}
            className={cn(
              "flex-1 resize-none rounded-md border border-bronze bg-iron px-3 py-2",
              "font-sans text-sm text-parchment placeholder:text-ash/50",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-1 focus-visible:ring-offset-onyx",
              "disabled:opacity-60"
            )}
          />
          {isBusy ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => stop()}
              className="border-bronze text-ash hover:border-ember hover:text-parchment hover:bg-iron"
            >
              Stop
            </Button>
          ) : (
            <Button
              type="submit"
              size="sm"
              disabled={!input.trim()}
              className="bg-ember text-onyx hover:bg-ember-bright disabled:opacity-40"
            >
              Send
            </Button>
          )}
        </div>
        <p className="mt-2 font-mono text-[10px] uppercase tracking-widest text-ash/50">
          Enter to send · Shift+Enter for newline
        </p>
      </form>
    </aside>
  )
}

// ─── Tool invocation sub-component (Step 7) ─────────────────────────────────

/**
 * Compact indicator for a tool invocation in the chat stream.
 * Shows a Database icon + tool name + state (loading/done/error).
 * Keeps the chat clean — the raw tool output is never shown; the model's
 * text summary is what the user reads.
 */
function ToolInvocationChip({
  part,
}: {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  part: any
}) {
  const toolName: string =
    part.toolName ?? part.type?.replace("tool-", "") ?? "unknown"
  const label = TOOL_LABELS[toolName] ?? toolName
  const state: string = part.state ?? "input-available"

  const isLoading =
    state === "input-streaming" || state === "input-available"
  const isError = state === "output-error"
  const isDone = state === "output-available"

  return (
    <div
      className={cn(
        "my-1 flex items-center gap-1.5 rounded border px-2 py-1",
        "font-mono text-[10px] uppercase tracking-widest",
        isLoading && "border-ember/30 text-ember-bright animate-pulse",
        isDone && "border-bronze/40 text-ash/60",
        isError && "border-rarity-copper/40 text-rarity-copper"
      )}
    >
      <Database className="h-3 w-3 shrink-0" />
      <span>
        {isLoading && `Querying ${label}…`}
        {isDone && `Loaded ${label}`}
        {isError && `Failed: ${label}`}
      </span>
    </div>
  )
}

// ─── Grounding sub-components (Step 6.5) ────────────────────────────────────

type PositionedCitation = GroundingCitation & {
  position: number
  footnoteIndex: number
}

/**
 * Renders the Sage's response text with inline footnote markers inserted
 * at the positions where cited claims appear.
 *
 * The markers look like: "Fighter has a score of 98 (92%)[1]"
 * They're styled as superscript links that visually connect to the
 * footnotes section below.
 */
function renderAnnotatedText(
  fullText: string,
  citations: PositionedCitation[],
): React.ReactNode {
  if (citations.length === 0) return <span>{fullText}</span>

  const parts: React.ReactNode[] = []
  let cursor = 0

  for (const cite of citations) {
    const claimEnd = cite.position + cite.claim.length

    // Text before this claim
    if (cite.position > cursor) {
      parts.push(
        <span key={`pre-${cite.footnoteIndex}`}>
          {fullText.slice(cursor, cite.position)}
        </span>,
      )
    }

    // The claim text itself
    parts.push(
      <span key={`claim-${cite.footnoteIndex}`}>
        {fullText.slice(cite.position, claimEnd)}
      </span>,
    )

    // Inline footnote marker
    parts.push(
      <sup
        key={`fn-${cite.footnoteIndex}`}
        className={cn(
          "ml-0.5 font-mono text-[9px] cursor-default",
          cite.grounded
            ? "text-rarity-gold"
            : "text-rarity-copper"
        )}
        title={`${cite.confidence}% — ${cite.explanation}`}
      >
        ({cite.confidence}%)
        <span className="text-ash/70">[{cite.footnoteIndex}]</span>
      </sup>,
    )

    cursor = claimEnd
  }

  // Remaining text after last citation
  if (cursor < fullText.length) {
    parts.push(<span key="tail">{fullText.slice(cursor)}</span>)
  }

  return <>{parts}</>
}

/**
 * Compact byline showing the headline AI grounding score + data sources.
 * Appears directly below the Sage message bubble.
 */
function GroundingByline({ grounding }: { grounding: GroundingResult }) {
  const tier = groundingTier(grounding.ai_grounding_confidence)
  const tierColorClass: Record<string, string> = {
    copper: "text-rarity-copper",
    silver: "text-rarity-silver",
    gold: "text-rarity-gold",
    platinum: "text-rarity-platinum",
    mithral: "text-rarity-mithral",
  }

  const Icon = grounding.ai_grounding_confidence >= 70 ? ShieldCheck : ShieldAlert

  return (
    <div className="flex items-center gap-1.5 mt-0.5 max-w-[90%]">
      <Icon
        className={cn("h-3 w-3 shrink-0", tierColorClass[tier])}
      />
      <span className="font-mono text-[10px] text-ash/70 truncate">
        <span className={cn("font-semibold", tierColorClass[tier])}>
          AI grounding {grounding.ai_grounding_confidence}%
        </span>
        {grounding.sources_available.length > 0 && (
          <span className="ml-1.5">
            · {grounding.sources_available.join(", ")}
          </span>
        )}
        <span className="ml-1.5 text-ash/50">
          · {grounding.algo_version}
        </span>
      </span>
    </div>
  )
}

/**
 * Footnotes section — renders below the byline. Each footnote shows
 * the claim text, its confidence score, the source, and a one-line
 * explanation. Styled like academic footnotes in a smaller font.
 */
function GroundingFootnotes({
  citations,
}: {
  citations: PositionedCitation[]
}) {
  if (citations.length === 0) return null

  return (
    <div className="max-w-[90%] mt-1 space-y-1.5 border-t border-bronze/30 pt-1.5">
      {citations.map((cite) => (
        <div
          key={cite.footnoteIndex}
          className="flex gap-1.5 font-mono text-[10px] leading-relaxed"
        >
          <span className="text-ash/50 shrink-0">
            [{cite.footnoteIndex}]
          </span>
          <div className="text-ash/70">
            <span
              className={cn(
                "font-semibold",
                cite.grounded ? "text-rarity-gold" : "text-rarity-copper"
              )}
            >
              {cite.confidence}%
            </span>
            <span className="mx-1">·</span>
            <span className="text-ash/50">{cite.source}</span>
            {(cite.explanation || cite.claim) && (
              <>
                <span className="mx-1">—</span>
                <span className="italic">
                  {cite.explanation || `"${cite.claim}"`}
                </span>
              </>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}
