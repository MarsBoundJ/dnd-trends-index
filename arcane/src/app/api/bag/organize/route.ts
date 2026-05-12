/*
 * Trusight — Bag of Holding organize endpoint.
 *
 * POSTs bag items + optional user instructions to Vertex AI Gemini
 * and returns a structured cluster organization suitable for
 * rendering as a report.
 *
 * This is Sage's "Council Chair" role expressed as synthesis:
 * the user has pinned a pile of items and wants them grouped into
 * meaningful sections for a report. Sage reads the pile, understands
 * the user's (optional) framing instruction, and returns 3-5 thematic
 * clusters — each with a title, optional description, and the ids of
 * the items that belong in it.
 *
 * Design notes:
 *   - Client-driven: bag items are POSTed in the request body, not
 *     read from Firestore. This keeps the endpoint working for
 *     anonymous users AND for signed-in users without any auth
 *     coupling. The Zustand store on the client is the source of
 *     truth for what's in the bag; this endpoint just interprets it.
 *   - Structured output: uses generateObject with a Zod schema so
 *     the response shape is parseable and never requires string
 *     repair. Gemini's structured-output mode guarantees the model
 *     returns JSON matching the schema.
 *   - Ephemeral by design: nothing is persisted. Every call is a
 *     fresh interpretation. Users can re-run with different
 *     instructions to get different organizations.
 *
 * Credential handling mirrors sage/route.ts — inline JSON env var
 * on Vercel, falls back to gcloud ADC for local dev.
 */

import { NextResponse } from "next/server"
import { generateObject } from "ai"
import { createVertex } from "@ai-sdk/google-vertex"
import { z } from "zod"

export const maxDuration = 45
export const dynamic = "force-dynamic"

// ─── Vertex client setup (same pattern as sage/route.ts) ─────────────────────

const jsonCreds = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON
const googleAuthOptions = jsonCreds
  ? { credentials: JSON.parse(jsonCreds) }
  : undefined

const vertex = createVertex({
  project: process.env.VERTEX_AI_PROJECT,
  location: process.env.VERTEX_AI_LOCATION ?? "us-central1",
  googleAuthOptions,
})

// ─── Structured output schema ────────────────────────────────────────────────

const organizeSchema = z.object({
  sections: z
    .array(
      z.object({
        title: z.string().describe(
          "Short, descriptive section heading (3-8 words). NOT generic like " +
            "'Group A' — should name the thematic thread connecting the items.",
        ),
        description: z
          .string()
          .optional()
          .describe(
            "One-sentence context explaining why these items belong together. " +
              "Optional — omit for self-explanatory sections.",
          ),
        itemIds: z
          .array(z.string())
          .min(1)
          .describe(
            "IDs of the items that belong in this section. Every input item " +
              "must appear in exactly one section (no duplicates, no omissions).",
          ),
      }),
    )
    .min(2)
    .max(8)
    .describe(
      "2-8 thematic sections. Default to 3-5 unless the input pile or the user's " +
        "instructions clearly warrant fewer or more.",
    ),
})

// ─── Request shape ───────────────────────────────────────────────────────────

interface BagItemForOrganize {
  id: string
  kind: "card" | "sage-answer" | "sage-thread"
  title: string
  excerpt: string
}

interface OrganizeRequestBody {
  items: BagItemForOrganize[]
  instructions?: string
}

// ─── System prompt ───────────────────────────────────────────────────────────

const SAGE_ORGANIZE_SYSTEM_PROMPT = `You are Sage of Trusight — a D&D trend intelligence oracle — acting in your Council Chair role as synthesist.

A user has pinned a pile of items to their Bag of Holding and wants them organized into thematic sections for a report. Your job is to read the items and return a clean cluster organization.

VOICE: The Strategist. Cool, tactical, precise. Section titles are noun-phrases, not sentences. Descriptions (when present) are one sentence, concrete, never breathless.

GROUPING RULES:
- Every input item appears in exactly ONE section (no duplicates, no omissions).
- Prefer 3-5 sections. Use 2 only for very small piles (< 6 items), 6-8 only for very diverse piles.
- Section titles NAME the thread that connects the items. NOT "Group A" / "Miscellaneous" / "Other Items" — if you'd write "Miscellaneous" you haven't found the real grouping yet, try harder.
- If the user provides organization instructions, FOLLOW THEM. Their framing beats your default heuristics.
- If no instructions, group by thematic resonance: shared subject, shared analytical lens, shared strategic implication. Not by item kind (cards vs. Sage answers) — the user can tell kinds apart visually; you're here for semantic structure.

SECTION TITLE EXAMPLES:
- "Universes Beyond candidate shortlist"
- "D&D Grow-Brand decline diagnostics"
- "Creator economy signals"
- "Mechanics-friction indicators from 2024 revision"
- "Digital-direct vs. tabletop pull-forward"

Return the organization as structured JSON matching the schema.`

// ─── Route handler ───────────────────────────────────────────────────────────

export async function POST(req: Request) {
  let body: OrganizeRequestBody
  try {
    body = await req.json()
  } catch {
    return NextResponse.json(
      { error: "Invalid JSON body" },
      { status: 400 },
    )
  }

  const { items, instructions } = body

  if (!Array.isArray(items) || items.length === 0) {
    return NextResponse.json(
      { error: "No items to organize" },
      { status: 400 },
    )
  }

  // Guard against absurd payloads — 80 items is already a huge pile for
  // a meaningful report; beyond that Gemini's context budget gets tight
  // and sections get unwieldy anyway.
  if (items.length > 80) {
    return NextResponse.json(
      { error: "Too many items (max 80). Unstow a few first." },
      { status: 400 },
    )
  }

  // Truncate excerpts to keep the prompt lean — 300 chars per item is
  // enough signal for grouping without blowing token budget.
  const truncatedItems = items.map((item) => ({
    ...item,
    excerpt: item.excerpt.length > 300
      ? item.excerpt.slice(0, 297) + "..."
      : item.excerpt,
  }))

  const userPromptParts = [
    instructions
      ? `User's organization instructions: "${instructions.trim()}"`
      : `No specific instructions — use your default thematic grouping.`,
    "",
    `Items to organize (${truncatedItems.length}):`,
    JSON.stringify(truncatedItems, null, 2),
  ]

  try {
    const result = await generateObject({
      model: vertex("gemini-2.5-flash"),
      system: SAGE_ORGANIZE_SYSTEM_PROMPT,
      prompt: userPromptParts.join("\n"),
      schema: organizeSchema,
    })

    // Sanity check: every input id should appear in the output, no duplicates.
    const inputIds = new Set(items.map((i) => i.id))
    const outputIds = new Set(
      result.object.sections.flatMap((s) => s.itemIds),
    )

    const missing = [...inputIds].filter((id) => !outputIds.has(id))
    const extra = [...outputIds].filter((id) => !inputIds.has(id))

    if (missing.length > 0 || extra.length > 0) {
      console.warn(
        "[bag/organize] item id mismatch — missing:",
        missing,
        "extra:",
        extra,
      )
      // Don't fail hard — downstream rendering will show missing items as
      // "unclustered" and ignore extras. But log for observability.
    }

    return NextResponse.json(result.object)
  } catch (err) {
    console.error("[bag/organize] Gemini call failed:", err)
    return NextResponse.json(
      {
        error:
          "Sage couldn't organize the bag right now. Try again in a moment, or use the default content-type grouping from the report page.",
      },
      { status: 500 },
    )
  }
}
