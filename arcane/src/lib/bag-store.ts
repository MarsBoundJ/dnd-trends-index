/*
 * Arcane Analytics — Bag of Holding store (Step 5 MVP).
 *
 * Zustand store with localStorage persistence. Stows three kinds of items:
 *
 *   1. "card"         — a data card the user pinned from a lens. We keep
 *                       enough metadata to re-render it as a summary row
 *                       on /collection. The card's plain-text `sageContext`
 *                       snapshot doubles as the data-at-stow-time record,
 *                       which is what the export pipeline (Step 15) will
 *                       eventually read.
 *
 *   2. "sage-answer"  — a single Q+A pair from a Sage conversation. Small,
 *                       self-contained, good unit for exec briefs.
 *
 *   3. "sage-thread"  — the entire current Sage conversation. Read-only
 *                       artifact: view and delete, but no "resume this
 *                       thread" until Firestore persistence in Step 11.
 *
 * Scope per FRONTEND_BUILD_STATUS.md Step 5:
 *   - localStorage only (no Firestore — Step 11)
 *   - stow-and-view-and-delete (no export — Step 15)
 *   - no drag-reorder (dnd-kit punted to later polish pass)
 *   - no Framer Motion bounce (Step 14)
 *
 * SSR / hydration note: Zustand's `persist` middleware reads localStorage
 * on the client only. Components that render a count-dependent value (like
 * a badge) must gate on `useBagHasHydrated()` to avoid SSR/CSR mismatch.
 */

import { create } from "zustand"
import { persist, createJSONStorage } from "zustand/middleware"

// ─── Types ───────────────────────────────────────────────────────────────────

export interface StowedCard {
  kind: "card"
  id: string
  title: string
  subtitle?: string
  lens?: string
  cardType?: string
  confidence: number
  /** Plain-text snapshot of the card's data at the time of stow. Doubles as
   *  the Sage page-context string. This is what Step 15's PDF export will
   *  render — so we save it, not the JSX children. */
  snapshot: string
  stowedAt: number
}

export interface StowedSageAnswer {
  kind: "sage-answer"
  id: string
  /** Which card/page the Sage was talking about when this answer was given. */
  pageContext: string
  question: string
  answer: string
  stowedAt: number
}

export interface StowedSageThread {
  kind: "sage-thread"
  id: string
  pageContext: string
  /** Chronological message history, assistant + user interleaved. */
  messages: Array<{ role: "user" | "assistant"; text: string }>
  stowedAt: number
}

export type StowedItem = StowedCard | StowedSageAnswer | StowedSageThread

// ─── Store ───────────────────────────────────────────────────────────────────

interface BagState {
  items: StowedItem[]
  /** Stow a card. If an item with the same `id` is already in the bag, this
   *  is a no-op — stow is idempotent so clicking twice doesn't duplicate. */
  stowCard: (card: Omit<StowedCard, "kind" | "stowedAt">) => void
  stowSageAnswer: (
    answer: Omit<StowedSageAnswer, "kind" | "stowedAt">
  ) => void
  stowSageThread: (
    thread: Omit<StowedSageThread, "kind" | "stowedAt">
  ) => void
  /** Remove any stowed item (any kind) by id. */
  unstow: (id: string) => void
  /** Check whether a card/answer/thread is already in the bag — lets the UI
   *  switch the Stow button to a "Stowed ✓" state. */
  isStowed: (id: string) => boolean
  clear: () => void
}

export const useBagStore = create<BagState>()(
  persist(
    (set, get) => ({
      items: [],

      stowCard: (card) => {
        if (get().items.some((i) => i.id === card.id)) return
        set((state) => ({
          items: [
            ...state.items,
            { kind: "card", ...card, stowedAt: Date.now() },
          ],
        }))
      },

      stowSageAnswer: (answer) => {
        if (get().items.some((i) => i.id === answer.id)) return
        set((state) => ({
          items: [
            ...state.items,
            { kind: "sage-answer", ...answer, stowedAt: Date.now() },
          ],
        }))
      },

      stowSageThread: (thread) => {
        if (get().items.some((i) => i.id === thread.id)) return
        set((state) => ({
          items: [
            ...state.items,
            { kind: "sage-thread", ...thread, stowedAt: Date.now() },
          ],
        }))
      },

      unstow: (id) =>
        set((state) => ({ items: state.items.filter((i) => i.id !== id) })),

      isStowed: (id) => get().items.some((i) => i.id === id),

      clear: () => set({ items: [] }),
    }),
    {
      name: "arcane:bag", // localStorage key
      version: 1,
      storage: createJSONStorage(() => localStorage),
    }
  )
)

// ─── Hydration gate ──────────────────────────────────────────────────────────
/**
 * Returns `true` once the Zustand persist middleware has rehydrated from
 * localStorage. Use this in any component that renders a value derived from
 * the bag (badge count, stow state) to avoid SSR/CSR hydration mismatches.
 *
 * Usage:
 *   const hydrated = useBagHasHydrated()
 *   const count = useBagStore((s) => s.items.length)
 *   return <span>{hydrated ? count : null}</span>
 */
import { useSyncExternalStore } from "react"

export function useBagHasHydrated(): boolean {
  // `useSyncExternalStore` is the canonical React 19 way to read from a
  // non-React data source without tripping hydration mismatches: the server
  // snapshot is always `false`, and the client subscribes via Zustand
  // persist's own `onFinishHydration` callback.
  return useSyncExternalStore(
    (onChange) => useBagStore.persist.onFinishHydration(onChange),
    () => useBagStore.persist.hasHydrated(),
    () => false
  )
}
