"use client"

/*
 * Trusight — Bag-of-Holding ↔ Firestore sync glue (Step 11).
 *
 * Mounted once in the root layout. Watches the Auth.js session and
 * orchestrates the localStorage ↔ Firestore lifecycle:
 *
 *   ┌─ "anon"    ─── sign-in ──▶ "syncing" ── merge done ──▶ "synced" ─┐
 *   │                                                                    │
 *   └───────────────────────── sign-out ◀────────────────────────────────┘
 *
 * On sign-in (or on mount with an already-signed-in session): POST local
 * items to /api/bag/merge, replace the store with the merged set, flip
 * syncStatus to "synced", register the BagSyncer so subsequent stow /
 * unstow mirror to the cloud.
 *
 * On sign-out: clear local items (so a different user on the same
 * browser doesn't inherit this user's bag), deregister the syncer,
 * reset syncStatus to "anon".
 *
 * Renders nothing — it's pure side-effect infrastructure.
 */

import React from "react"
import { useSession } from "next-auth/react"

import {
  registerBagSyncer,
  useBagStore,
  useBagHasHydrated,
  type StowedItem,
} from "@/lib/bag-store"

export function BagSync() {
  const { data: session, status } = useSession()
  const hydrated = useBagHasHydrated()
  const syncStatus = useBagStore((s) => s.syncStatus)
  const setSyncStatus = useBagStore((s) => s.setSyncStatus)
  const setItems = useBagStore((s) => s.setItems)
  const clear = useBagStore((s) => s.clear)

  React.useEffect(() => {
    // Wait for Zustand's persist middleware to finish reading localStorage.
    // Syncing before hydration would merge an empty local bag into the
    // cloud on every reload — blowing away any legitimate local items.
    if (!hydrated) return

    // Auth still loading → do nothing. Next render will settle it.
    if (status === "loading") return

    if (status === "unauthenticated") {
      // Signed out. Deregister syncer, wipe local cache, mark anon.
      // Wiping local is important on shared machines (user B signs in
      // after user A signs out shouldn't inherit A's bag via merge).
      registerBagSyncer(null)
      if (syncStatus !== "anon") {
        clear()
        setSyncStatus("anon")
      }
      return
    }

    // Authenticated path.
    if (syncStatus === "anon") {
      setSyncStatus("syncing")
      const localItems = useBagStore.getState().items
      ;(async () => {
        try {
          const res = await fetch("/api/bag/merge", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ items: localItems }),
          })
          if (!res.ok) {
            console.error(
              `[bag-sync] merge returned ${res.status} ${res.statusText}`,
            )
            // Leave syncStatus in "syncing" so we don't register the
            // mirroring syncer — avoids silently half-working writes.
            // Next reload will retry.
            return
          }
          const body = (await res.json()) as { items: StowedItem[] }
          setItems(body.items)
          registerBagSyncer({
            postItem: async (item) => {
              const r = await fetch("/api/bag", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(item),
              })
              if (!r.ok) {
                throw new Error(`POST /api/bag ${r.status}`)
              }
            },
            deleteItem: async (id) => {
              const r = await fetch(
                `/api/bag?id=${encodeURIComponent(id)}`,
                { method: "DELETE" },
              )
              if (!r.ok) {
                throw new Error(`DELETE /api/bag ${r.status}`)
              }
            },
          })
          setSyncStatus("synced")
        } catch (err) {
          console.error("[bag-sync] merge failed:", err)
        }
      })()
    }
    // If already "synced" and session still authed: nothing to do.
    // The registered syncer stays live across re-renders because
    // registerBagSyncer writes to a module-scoped singleton.
  }, [hydrated, status, syncStatus, session?.user?.id, clear, setItems, setSyncStatus])

  return null
}
