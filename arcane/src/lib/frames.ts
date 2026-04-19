/*
 * Arcane Analytics — Frame abstraction (Step 9.5).
 *
 * A "Frame" is a switchable interpretive prior used by Track D articles and
 * Sage when answering questions about signals in the frame's domain. Frames
 * live in Firestore (`frames/{frameId}`) as config docs rather than in code,
 * so updating Hasbro's FY27 strategy — or pitching to a different buyer —
 * is a one-doc edit rather than a release.
 *
 * Frame shape is documented in
 *   `project_tracks_frames_roadmap.md`  (technical spine)
 *   `project_hasbro_pitch_problems_solutions.md`  (commercial spine)
 * both in the project memory folder.
 *
 * This module is `"server-only"` — Firestore admin reads happen from route
 * handlers and server actions, never from client bundles.
 */

import "server-only"

import { z } from "zod"
import { getDb } from "@/lib/firebase-admin"

// ─── Schemas ────────────────────────────────────────────────────────────────

const StrategicBuildingBlockSchema = z.object({
  id: z.string().min(1),
  label: z.string().min(1),
  rubric: z.string().min(1),
})

const BenchmarkSchema = z.record(z.string(), z.union([z.number(), z.string()]))

const RiskOnWatchSchema = z.object({
  id: z.string().min(1),
  fact: z.string().min(1),
})

/**
 * Tone distribution for Track D article generation. Integer ratio of
 * deck-ready to sharp articles — `{ deck_ready: 6, sharp: 1 }` means
 * for every 7 articles, 6 are polished/exec-safe and 1 has a sharper
 * edge. Any integer ratio works (3:1, 4:1, 5:1, 6:1…); retune by
 * editing the frame's Firestore doc, no code change needed.
 */
const ToneDistributionSchema = z.object({
  deck_ready: z.number().int().nonnegative(),
  sharp: z.number().int().nonnegative(),
})

const GuardrailSchema = z.string().min(1)

export const FrameSchema = z.object({
  frame_id: z.string().min(1),
  label: z.string().min(1),
  /** Empty array means "no active corporate-strategy frame" (Pure Data baseline). */
  source_docs: z.array(z.string()).default([]),
  /**
   * Sage injects this when a user question matches the frame's domain.
   * Empty string = no worldview context; Sage answers from her own voice
   * without leaning on a frame. Required field but `""` is valid.
   */
  worldview_summary: z.string(),
  /** Playing-to-Win-shaped interpretive priors. Empty for Pure Data. */
  strategic_building_blocks: z.array(StrategicBuildingBlockSchema).default([]),
  /** Per-buyer brand-portfolio taxonomy (e.g. Grow/Optimize/Reinvent). */
  portfolio_taxonomy: z.array(z.string()).default([]),
  /** Per-buyer structural taxonomy (e.g. GEM² = Gamified/Entertainment-driven/Multi-purchase/Multi-generational). */
  structural_taxonomy: z.array(z.string()).default([]),
  /** Corpus facts: named Grow-tier brands (Hasbro) or equivalent priority list. */
  named_grow_brands: z.array(z.string()).default([]),
  /** Corpus facts: named GEM² franchises (Hasbro) or equivalent structural list. */
  named_gem2_franchises: z.array(z.string()).default([]),
  /** Named reference numbers for "what good looks like" calibration. */
  benchmarks: BenchmarkSchema.default({}),
  /** Active risks the frame wants Track D to watch for. */
  risks_on_watch: z.array(RiskOnWatchSchema).default([]),
  /** Corpus facts about the current licensing pipeline (Universes Beyond etc.). */
  active_universes_beyond_pipeline: z.array(z.string()).default([]),
  /** Corpus facts about current partnership pipeline. */
  active_partnership_pipeline: z.array(z.string()).default([]),
  /** Corpus facts about in-house digital game slate. */
  in_house_digital_slate: z.array(z.string()).default([]),
  /** 6:1 default, tunable to 3:1 / 4:1 / 5:1 via Firestore edit. */
  tone_distribution: ToneDistributionSchema.default({ deck_ready: 6, sharp: 1 }),
  /** Semantic guardrails Track D articles must respect (e.g. fan_vs_shareholder). */
  guardrails: z.array(GuardrailSchema).default([]),
  /**
   * Null / empty baseline. When the active frame is "pure-data" (empty
   * worldview_summary + empty strategic_building_blocks), Track D is off
   * and the Council writes Track B articles using their own-expertise frame.
   */
})

export type Frame = z.infer<typeof FrameSchema>

// ─── Active-frame pointer ───────────────────────────────────────────────────

const ActiveFrameMetaSchema = z.object({
  activeFrameId: z.string().min(1),
})

const ACTIVE_META_DOC = "_meta"
const ACTIVE_META_FIELD = "activeFrameId"

// ─── Collection path helpers ────────────────────────────────────────────────

/** Top-level collection holding all frame docs + the `_meta` pointer. */
export const FRAMES_COLLECTION = "frames"

/** Side-collection under a frame doc for large corpus chunks. */
export function corpusCollectionPath(frameId: string): string {
  return `${FRAMES_COLLECTION}/${frameId}/corpus`
}

/** Side-collection for per-frame concept mappings (tier + structural tags). */
export function conceptMappingsCollectionPath(frameId: string): string {
  return `${FRAMES_COLLECTION}/${frameId}/concept_mappings`
}

// ─── Loaders ────────────────────────────────────────────────────────────────

/**
 * Returns the id of the frame currently flagged as active in Firestore, or
 * null if nothing is set. Store the pointer as a single `_meta` doc rather
 * than a boolean on each frame so activation is atomic (single-writer).
 */
export async function getActiveFrameId(): Promise<string | null> {
  const snap = await getDb()
    .collection(FRAMES_COLLECTION)
    .doc(ACTIVE_META_DOC)
    .get()
  if (!snap.exists) return null
  const data = snap.data()
  if (!data) return null
  const parsed = ActiveFrameMetaSchema.safeParse(data)
  if (!parsed.success) return null
  return parsed.data[ACTIVE_META_FIELD]
}

/**
 * Atomically set the active frame. The target frame must exist. Writes a
 * single `frames/_meta` doc. Returns the new active id on success.
 */
export async function setActiveFrameId(frameId: string): Promise<string> {
  const target = await getDb()
    .collection(FRAMES_COLLECTION)
    .doc(frameId)
    .get()
  if (!target.exists) {
    throw new Error(`frames/${frameId} does not exist`)
  }
  await getDb()
    .collection(FRAMES_COLLECTION)
    .doc(ACTIVE_META_DOC)
    .set({ [ACTIVE_META_FIELD]: frameId })
  return frameId
}

/**
 * Load one frame by id. Returns null if the doc is missing. Validates
 * the payload against FrameSchema — malformed docs surface a thrown
 * ZodError for the caller to handle, rather than propagating a half-valid
 * object into prompt generation.
 */
export async function getFrameById(frameId: string): Promise<Frame | null> {
  // Reserved: the _meta doc is never a Frame.
  if (frameId === ACTIVE_META_DOC) return null

  const snap = await getDb()
    .collection(FRAMES_COLLECTION)
    .doc(frameId)
    .get()
  if (!snap.exists) return null
  const raw = snap.data()
  if (!raw) return null
  return FrameSchema.parse(raw)
}

/**
 * Load the currently active frame. Returns null if no active frame is set
 * (pre-seed state or deliberate "off" state). Callers decide whether null
 * means "skip corporate-strategy framing entirely" (Pure Data path) or
 * "fail" (rare — usually it's a fallback signal, not an error).
 */
export async function getActiveFrame(): Promise<Frame | null> {
  const id = await getActiveFrameId()
  if (!id) return null
  return getFrameById(id)
}

/**
 * List all frames (excluding the `_meta` pointer). Used by the admin panel
 * at `/admin/frames`.
 */
export async function listFrames(): Promise<Frame[]> {
  const snap = await getDb()
    .collection(FRAMES_COLLECTION)
    .get()
  const frames: Frame[] = []
  for (const doc of snap.docs) {
    if (doc.id === ACTIVE_META_DOC) continue
    const parsed = FrameSchema.safeParse(doc.data())
    if (parsed.success) frames.push(parsed.data)
  }
  // Stable sort by label for UI predictability.
  frames.sort((a, b) => a.label.localeCompare(b.label))
  return frames
}
