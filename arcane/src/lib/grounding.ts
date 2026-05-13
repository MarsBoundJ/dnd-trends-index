/*
 * Trusight — AI Grounding Engine (Step 6.5).
 *
 * Scores how well AI-generated text (Sage responses, articles) is
 * grounded in the source data that was provided as context.
 *
 * Architecture:
 *   1. Sage streams a response using pageContext as its system prompt.
 *   2. Once complete, the client calls /api/sage/ground with the
 *      generated text + the same pageContext.
 *   3. This module makes a STRUCTURED Gemini call that returns:
 *        - ai_grounding_confidence (0-100 headline score)
 *        - citations[] with per-claim scores, sources, explanations
 *   4. The sage panel renders inline footnote markers + a footnotes
 *      section at the bottom of the message.
 *
 * The grounding check is a SECOND LLM call, not a modification of the
 * original Sage call. This keeps the streaming UX fast and lets the
 * grounding check run asynchronously after the user has already seen
 * the response.
 *
 * See CONFIDENCE_METHODOLOGY.md §6.3 for the design rationale and
 * FRONTEND_DESIGN_SPEC.md §5.1 for the two-layer confidence model.
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/**
 * A single grounded citation — one factual claim in the generated text
 * that the grounding check has scored against the source context.
 */
export interface GroundingCitation {
  /** The exact claim text as it appears in the generated response. */
  claim: string
  /** 0-100 confidence that this claim is supported by the source data. */
  confidence: number
  /** Which data source(s) support this claim (e.g. "Google Trends leaderboard"). */
  source: string
  /** One-sentence explanation of why the claim is/isn't grounded. */
  explanation: string
  /** Whether the claim is well-grounded (confidence >= 70). */
  grounded: boolean
}

/**
 * Full grounding result for one piece of generated text.
 */
export interface GroundingResult {
  /** Headline AI grounding score (0-100). For AI cards, displayed confidence = min(data, ai_grounding). */
  ai_grounding_confidence: number
  /** Per-claim citations with scores and explanations. */
  citations: GroundingCitation[]
  /** Data sources mentioned in the context that were available for grounding. */
  sources_available: string[]
  /** Algorithm version for the grounding check prompt. */
  algo_version: string
}

// ─── Grounding prompt ───────────────────────────────────────────────────────

/**
 * System prompt for the grounding check. This is a separate, structured
 * call — not part of the Sage's conversational loop.
 *
 * Key design choices:
 * - We ask the model to return JSON (structured output), not prose.
 * - We tell it to identify FACTUAL/NUMERIC claims only, not opinions
 *   or framing. "Fighter is a great class" → skip. "Fighter has a
 *   score of 98" → cite.
 * - We cap at 5 citations to avoid footnote fatigue per Yorri's UX
 *   direction ("too many confidence footnotes would be annoying").
 * - The headline score is the MINIMUM of per-claim confidences, matching
 *   the "chain = weakest link" philosophy from data_confidence.
 */
export const GROUNDING_SYSTEM_PROMPT = `You are a fact-checking assistant for a D&D trend intelligence application called Trusight.

Your job: given a piece of AI-generated text and the source data context that was available when it was written, identify the key factual claims and score how well each is grounded in the source data.

Rules:
1. Only flag FACTUAL or NUMERIC claims — statements about data, rankings, percentages, trends, comparisons. Skip opinions, framing, recommendations, and general D&D knowledge.
2. Return at most 5 citations (the most important factual claims). If fewer than 5 factual claims exist, return fewer.
3. For each claim, score confidence 0-100:
   - 90-100: Claim directly matches a number or fact in the source data
   - 70-89: Claim is a reasonable interpretation of the source data
   - 40-69: Claim has partial support but extrapolates beyond the data
   - 0-39: Claim has no support in the source data (likely hallucinated)
4. "grounded" is true when confidence >= 70, false otherwise.
5. The "claim" field must be the EXACT substring from the generated text — do not paraphrase.
6. "source" should name the specific data source (e.g. "Google Trends leaderboard", "Category Heat chart data").
7. "sources_available" should list the data sources that were present in the context (e.g. ["Google Trends", "Category Heat"]).
8. "ai_grounding_confidence" is the MINIMUM of all per-claim confidence scores. If no factual claims are found, return 95 (the text is grounded by being purely conversational/opinion).

Return valid JSON matching the schema exactly. No markdown fences, no explanation outside the JSON.`

/**
 * JSON schema for the structured output. Passed to Gemini via
 * response_schema so the model returns parseable JSON every time.
 */
export const GROUNDING_RESPONSE_SCHEMA = {
  type: "object" as const,
  properties: {
    ai_grounding_confidence: {
      type: "number" as const,
      description: "Headline AI grounding score 0-100. Minimum of per-claim scores.",
    },
    citations: {
      type: "array" as const,
      items: {
        type: "object" as const,
        properties: {
          claim: {
            type: "string" as const,
            description: "Exact substring from the generated text.",
          },
          confidence: {
            type: "number" as const,
            description: "0-100 confidence that this claim is supported.",
          },
          source: {
            type: "string" as const,
            description: "Which data source supports this claim.",
          },
          explanation: {
            type: "string" as const,
            description: "One-sentence explanation.",
          },
          grounded: {
            type: "boolean" as const,
            description: "True if confidence >= 70.",
          },
        },
        required: ["claim", "confidence", "source", "explanation", "grounded"],
      },
    },
    sources_available: {
      type: "array" as const,
      items: { type: "string" as const },
      description: "Data sources present in the context.",
    },
  },
  required: ["ai_grounding_confidence", "citations", "sources_available"],
}

// ─── Client-side helpers ────────────────────────────────────────────────────

/**
 * Find where a citation's claim text appears in the full response,
 * returning the character index. Returns -1 if not found (fuzzy match
 * failed — the citation will still appear in the footnotes section
 * but won't get an inline marker).
 */
export function findClaimPosition(
  fullText: string,
  claimText: string,
): number {
  // Exact match first
  const exact = fullText.indexOf(claimText)
  if (exact !== -1) return exact

  // Try case-insensitive
  const lowerFull = fullText.toLowerCase()
  const lowerClaim = claimText.toLowerCase()
  const caseInsensitive = lowerFull.indexOf(lowerClaim)
  if (caseInsensitive !== -1) return caseInsensitive

  // Try trimmed (the LLM sometimes adds/drops trailing punctuation)
  const trimmed = claimText.replace(/[.,;:!?]+$/, "")
  if (trimmed.length > 10) {
    const trimmedIdx = lowerFull.indexOf(trimmed.toLowerCase())
    if (trimmedIdx !== -1) return trimmedIdx
  }

  return -1
}

/**
 * Given a GroundingResult, return only the citations that have inline
 * markers (i.e. their claim text was found in the response). Sorted
 * by position in the text so footnote numbers are sequential.
 */
export function positionedCitations(
  fullText: string,
  result: GroundingResult,
): (GroundingCitation & { position: number; footnoteIndex: number })[] {
  return result.citations
    .map((c) => ({
      ...c,
      position: findClaimPosition(fullText, c.claim),
      footnoteIndex: 0, // assigned below
    }))
    .filter((c) => c.position !== -1)
    .sort((a, b) => a.position - b.position)
    .map((c, i) => ({ ...c, footnoteIndex: i + 1 }))
}

/**
 * Confidence tier for the AI grounding score, matching the same metal
 * ladder used by data_confidence. Duplicated here (rather than importing
 * from card-chrome.tsx) because card-chrome is "use client" and this
 * module is used by both server and client code.
 */
export function groundingTier(
  score: number,
): "copper" | "silver" | "gold" | "platinum" | "mithral" {
  if (score >= 95) return "mithral"
  if (score >= 90) return "platinum"
  if (score >= 80) return "gold"
  if (score >= 70) return "silver"
  return "copper"
}
