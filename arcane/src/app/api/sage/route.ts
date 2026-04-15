/*
 * Arcane Analytics — Sage route handler (Step 4 MVP).
 *
 * Streams a Vertex AI Gemini response for the Sage chat panel. Page context
 * is passed from the client on every request and injected as a system message
 * so the model knows which lens/card the user is currently looking at.
 *
 * Step 4 scope: no tool calling (Step 7), no Firestore logging, no voice
 * selection (defaults to "Strategist" tone in the system prompt).
 *
 * Auth: @ai-sdk/google-vertex uses Application Default Credentials. On local
 * dev, run `gcloud auth application-default login` or set
 * GOOGLE_APPLICATION_CREDENTIALS to a service-account key path.
 */

import { convertToModelMessages, streamText, type UIMessage } from "ai"
import { createVertex } from "@ai-sdk/google-vertex"

// Allow streaming responses up to 30 seconds.
export const maxDuration = 30

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

// The Strategist is the default Sage voice until voice-selection lands in a
// later step. Cool, tactical, number-driven, never breathless.
const SAGE_SYSTEM_PROMPT = `You are the Sage of Arcane Analytics — a D&D trend intelligence oracle.

Voice: The Strategist. Cool, tactical, data-driven. Short sentences. Concrete
numbers when you have them. Never breathless or hype-y. You explain *why*
something is moving, not just *that* it is.

You are an in-app assistant embedded in the Arcane Analytics UI. The user is
currently looking at a specific page or card — its context will be provided
below as "Page context". Use it to ground your answers. If the user's
question is not about the visible data, answer from your own knowledge of
tabletop D&D, content creators, and the hobby's trend landscape.

Keep answers under ~150 words unless the user explicitly asks for depth.
Never invent numbers. If the context is missing a stat the user asks about,
say so plainly.`

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
  })

  return result.toUIMessageStreamResponse({
    onError: (error) => {
      // Surface enough of the failure for dev-time debugging. Step 6+ will
      // replace this with a user-safe generic message.
      if (error instanceof Error) return error.message
      return "Sage failed to respond."
    },
  })
}
