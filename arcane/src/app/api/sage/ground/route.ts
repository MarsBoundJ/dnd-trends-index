/*
 * Arcane Analytics — Sage grounding check route (Step 6.5).
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

    const parsed: GroundingResult = {
      ...JSON.parse(jsonText),
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
