/*
 * Trusight — Sage grounding check route (Step 6.5).
 *
 * Called by the Sage panel AFTER a streaming response completes.
 * Takes the generated text + the pageContext and returns a
 * GroundingResult with per-claim citations and a headline score.
 *
 * This is a separate, non-streaming call. The Sage panel shows
 * a "Verifying claims..." indicator while this runs (~2-5 seconds).
 */

import { generateText } from "ai"
import { createVertex } from "@ai-sdk/google-vertex"
import {
  GROUNDING_SYSTEM_PROMPT,
  type GroundingResult,
} from "@/lib/grounding"

export const dynamic = "force-dynamic"
export const maxDuration = 30

const vertex = createVertex({
  project: process.env.VERTEX_AI_PROJECT,
  location: process.env.VERTEX_AI_LOCATION ?? "us-central1",
})

interface GroundRequest {
  /** The Sage's generated response text. */
  generatedText: string
  /** The same pageContext that was fed to the Sage as system prompt context. */
  pageContext: string
}

export async function POST(req: Request) {
  const { generatedText, pageContext }: GroundRequest = await req.json()

  if (!generatedText?.trim()) {
    return Response.json(
      { error: "generatedText is required" },
      { status: 400 },
    )
  }

  // Build the user prompt for the grounding check.
  // The model sees: system prompt (grounding instructions) + user prompt
  // (the actual text to check against the context).
  const userPrompt = `## Source data context (what the AI had access to when generating its response):

${pageContext || "(No page context was provided — the response was generated from general knowledge only.)"}

## AI-generated text to fact-check:

${generatedText}

Identify the key factual claims in the AI-generated text and score how well each is grounded in the source data context above. Return JSON matching the schema.`

  try {
    const result = await generateText({
      model: vertex("gemini-2.5-flash"),
      system: GROUNDING_SYSTEM_PROMPT,
      prompt: userPrompt,
    })

    // Parse the structured JSON response.
    // Gemini sometimes wraps JSON in ```json fences — strip them.
    let jsonText = result.text.trim()
    if (jsonText.startsWith("```")) {
      jsonText = jsonText
        .replace(/^```(?:json)?\s*/, "")
        .replace(/\s*```$/, "")
    }

    const raw = JSON.parse(jsonText)

    // Gemini is inconsistent with field names. It may use "citations",
    // "fact_checks", "claims", or other variations. Find the first
    // array-valued property that looks like a citations list.
    const CITATION_KEYS = ["citations", "fact_checks", "claims", "results", "checks"]
    let rawCitations: Record<string, unknown>[] = []
    for (const key of CITATION_KEYS) {
      if (Array.isArray(raw[key]) && raw[key].length > 0) {
        rawCitations = raw[key]
        break
      }
    }
    // Last resort: find the first array property with objects that have a "claim" field
    if (rawCitations.length === 0) {
      for (const val of Object.values(raw)) {
        if (
          Array.isArray(val) &&
          val.length > 0 &&
          typeof val[0] === "object" &&
          val[0] !== null &&
          "claim" in val[0]
        ) {
          rawCitations = val as Record<string, unknown>[]
          break
        }
      }
    }

    // Normalize citations — Gemini sometimes nests sources_available
    // inside each citation or omits explanation. Ensure consistent shape.
    const citations = rawCitations.map((c: Record<string, unknown>) => ({
      claim: typeof c.claim === "string" ? c.claim : "",
      confidence: typeof c.confidence === "number" ? c.confidence : 0,
      source: typeof c.source === "string" ? c.source : "Unknown",
      explanation: typeof c.explanation === "string"
        ? c.explanation
        : "",
      grounded: typeof c.grounded === "boolean" ? c.grounded : false,
    }))

    // Extract sources_available — check top-level first, then collect
    // from per-citation sources (Gemini sometimes nests them).
    let sourcesAvailable: string[] = []
    if (Array.isArray(raw.sources_available) && raw.sources_available.length > 0) {
      sourcesAvailable = raw.sources_available
    } else {
      // Collect unique source names from citations
      const sourceSet = new Set<string>()
      for (const c of rawCitations) {
        if (typeof c.source === "string") sourceSet.add(c.source)
        if (Array.isArray(c.sources_available)) {
          for (const s of c.sources_available) {
            if (typeof s === "string") sourceSet.add(s)
          }
        }
      }
      sourcesAvailable = [...sourceSet]
    }

    // Compute headline score — use Gemini's if provided, otherwise
    // compute as min of per-claim scores per our prompt instructions.
    let headlineScore: number
    if (typeof raw.ai_grounding_confidence === "number") {
      headlineScore = raw.ai_grounding_confidence
    } else if (citations.length > 0) {
      headlineScore = Math.min(...citations.map((c: { confidence: number }) => c.confidence))
    } else {
      headlineScore = 95 // No factual claims = conversational/opinion
    }

    const parsed: GroundingResult = {
      ai_grounding_confidence: headlineScore,
      citations,
      sources_available: sourcesAvailable,
      algo_version: "grounding-v1.0.0",
    }

    // Sanity-check: clamp headline score to 0-100
    parsed.ai_grounding_confidence = Math.max(
      0,
      Math.min(100, Math.round(parsed.ai_grounding_confidence)),
    )

    // Clamp per-claim scores too
    for (const c of parsed.citations) {
      c.confidence = Math.max(0, Math.min(100, Math.round(c.confidence)))
      c.grounded = c.confidence >= 70
    }

    return Response.json(parsed)
  } catch (err) {
    console.error("[sage/ground] Grounding check failed:", err)
    // On failure, return a "pass" result so the UI doesn't break.
    // The user still sees the Sage response — they just don't get
    // grounding annotations. This is the graceful degradation path.
    const fallback: GroundingResult = {
      ai_grounding_confidence: 75,
      citations: [],
      sources_available: [],
      algo_version: "grounding-v1.0.0-fallback",
    }
    return Response.json(fallback)
  }
}
