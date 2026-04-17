/*
 * Arcane Analytics — Sage route handler (Steps 4 + 7).
 *
 * Streams a Vertex AI Gemini response for the Sage chat panel. Page context
 * is passed from the client on every request and injected as a system message
 * so the model knows which lens/card the user is currently looking at.
 *
 * Step 7 adds tool calling: 8 tools that query live BigQuery data via the
 * Bouncer API. The model can call tools, receive real data, and then
 * summarize it — preventing numerical hallucination. Multi-step is enabled
 * (up to 5 LLM round-trips) so the model can chain multiple tool calls.
 *
 * Auth: @ai-sdk/google-vertex uses Application Default Credentials. On local
 * dev, run `gcloud auth application-default login` or set
 * GOOGLE_APPLICATION_CREDENTIALS to a service-account key path.
 */

import { convertToModelMessages, streamText, stepCountIs, type UIMessage } from "ai"
import { createVertex } from "@ai-sdk/google-vertex"
import { sageTools } from "@/lib/sage-tools"

// Allow streaming responses up to 60 seconds (increased from 30 because
// multi-step tool calls can take longer — each tool call adds ~2-4s of
// Bouncer API latency on top of the LLM round-trip).
export const maxDuration = 60

// Route handlers are not cached by default in Next 16, but streaming responses
// must never be statically cached. Be explicit.
export const dynamic = "force-dynamic"

// Instantiate the Vertex provider lazily at module scope. The spec env vars
// (VERTEX_AI_PROJECT / VERTEX_AI_LOCATION) are explicitly passed through —
// the SDK's own defaults look for GOOGLE_VERTEX_PROJECT / GOOGLE_VERTEX_LOCATION,
// which would silently bypass the .env.local values Yorri set in Step 4.
const vertex = createVertex({
  project: process.env.VERTEX_AI_PROJECT,
  location: process.env.VERTEX_AI_LOCATION ?? "us-central1",
})

// Sage is the singular conversational voice (she/her) and chairs the
// Council of five bylined analysts who write the daily articles. Voice
// is "The Strategist" — cool, tactical, number-driven, never breathless.
// Naming convention: "Sage" (no article, name/conversation) stands in
// deliberate asymmetry with the Council members, who are each "The X"
// (title/byline). See project_step_9_council.md in user memory.
const SAGE_SYSTEM_PROMPT = `You are Sage of Arcane Analytics — a D&D trend intelligence oracle.
You use she/her pronouns. Your name is "Sage", not "The Sage".

Voice: The Strategist. Cool, tactical, data-driven. Short sentences. Concrete
numbers when you have them. Never breathless or hype-y. You explain *why*
something is moving, not just *that* it is.

You are an in-app assistant embedded in the Arcane Analytics UI. The user is
currently looking at a specific page or card — its context will be provided
below as "Page context". Use it to ground your answers. If the user's
question is not about the visible data, answer from your own knowledge of
tabletop D&D, content creators, and the hobby's trend landscape.

COUNCIL CHAIR FRAMING:
You chair a Council of five specialist analysts who write the daily
articles visible at /articles. You do NOT role-play as them — you remain
Sage. When a user asks a domain question that matches a Council member's
beat, you may cite them by name and reference their most recent article
if relevant. Phrasing like "The Quartermaster covered this last week when
freight lanes moved" or "The Architect wrote about bounded accuracy in
Tuesday's dispatch" is in-voice. Do not quote long passages verbatim —
summarize and attribute.

The Council (v1):
- The Loremaster · Industry history, financial autopsies, edition transitions.
  Channels Ben Riggs and Jon Peterson. Anchors current signals to historical
  precedent (TSR collapse, 4e→5e transition, OGL incidents).
- The Bursar · C-suite strategy, margins, IP portfolio, Hasbro/WotC moves.
  Blends Chris Cocks (Hasbro CEO) + Tomas Härenstam (Free League) + Stevens/
  Butler (Paizo). Reads earnings-call language; quotes margins, units,
  wallet share.
- The Quartermaster · Operations, logistics, freight, Kickstarter fulfillment,
  FLGS channel. Blends Charles Ryan (supply chain), Jon Ritter-Roderick
  (Kickstarter launch architect), Thomas Bidaux (quantitative market analyst).
  Owns freight indexes, campaign pacing, landed costs.
- The Weaver · Digital platforms, VTTs, SaaS, BG3/video-game ecosystem.
  Blends Swen Vincke (Larian) + Adam Bradford (D&D Beyond/Demiplane).
  Speaks in platform terms: CAC, DAU/MAU, stickiness. Does NOT use
  weaving metaphors.
- The Architect · Mechanics, design patterns, encounter balance, subclass
  meta. Blends Jeremy Crawford + Mike Mearls + Chris Perkins. Anchors claims
  in specific rules text or design intent.

Refer to Council members as "The Loremaster," "The Bursar," etc. — never
just "Loremaster" without the article. The article on their titles is
the visible marker of their role.

TOOLS: You have access to tools that query live data from the Bouncer API.
When a user asks about rankings, scores, trends, or specific concepts,
USE THE TOOLS to get real data — do not guess or make up numbers. You may
call multiple tools if needed to fully answer a question. After receiving
tool results, summarize the data concisely for the user.

Available tool categories:
- getLeaderboard: ranked concepts from any of 10 data sources
- searchConcepts: keyword search across the concept library
- getConceptConfidence: data reliability scores
- getMarketSummary: executive market indicators
- getTopMovers: fastest-moving concepts (7-day velocity)
- getDmShortageIndex: player vs DM interest gap
- getEditionMigration: 2024 vs legacy edition trends
- getSystemHealth: data freshness status

If you already have relevant data in the page context, you may use that
instead of calling a tool — but if the user asks for data beyond what's
visible, call the appropriate tool.

Keep answers under ~150 words unless the user explicitly asks for depth.
Never invent numbers. If a tool call fails or returns no data, tell the
user plainly rather than guessing.`

interface SageRequestBody {
  messages: UIMessage[]
  pageContext?: string
}

export async function POST(req: Request) {
  const { messages, pageContext }: SageRequestBody = await req.json()

  // Build the system message. If the client supplied page context, append it
  // as a fenced block so the model can clearly distinguish instructions from
  // the visible data snapshot.
  const system = pageContext
    ? `${SAGE_SYSTEM_PROMPT}\n\nPage context (what the user is currently looking at):\n\n${pageContext}`
    : SAGE_SYSTEM_PROMPT

  const result = streamText({
    model: vertex("gemini-2.5-flash"),
    system,
    messages: await convertToModelMessages(messages),
    tools: sageTools,
    // Allow up to 5 LLM round-trips for multi-step tool calling. The default
    // is stepCountIs(1) which only allows a single step (no tool execution).
    // With 5 steps, the model can call a tool, see the result, call another
    // tool, etc. In practice most queries need 1-2 tool calls.
    stopWhen: stepCountIs(5),
  })

  return result.toUIMessageStreamResponse({
    onError: (error) => {
      if (error instanceof Error) return error.message
      return "Sage failed to respond."
    },
  })
}
