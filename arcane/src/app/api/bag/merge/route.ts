/*
 * Trusight — Bag of Holding sign-in merge (Step 11).
 *
 * Called once per sign-in, from the client, with the user's current
 * localStorage items. Server reads the existing Firestore items,
 * union-dedupes by id (cloud entry wins on conflict — preserves the
 * persisted `stowedAt` as authoritative), writes any local-only
 * items to Firestore, and returns the full merged set so the client
 * can replace its in-memory store.
 *
 * Design choice: silent union. No "merge or replace" prompt — the
 * localStorage items a user just stowed anonymously have the same
 * identity (`id`) as anything they already had in the cloud, so
 * dedupe is the natural behavior. Anything genuinely new from the
 * anon session gets uploaded.
 */

import { NextResponse } from "next/server"
import { auth } from "../../../../../auth"
import { getDb } from "@/lib/firebase-admin"

export const dynamic = "force-dynamic"

interface BagItem {
  kind: "card" | "sage-answer" | "sage-thread"
  id: string
  [key: string]: unknown
}

interface MergeBody {
  items: BagItem[]
}

export async function POST(req: Request) {
  const session = await auth()
  if (!session?.user?.id) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 })
  }

  let body: MergeBody
  try {
    body = (await req.json()) as MergeBody
  } catch {
    return NextResponse.json({ error: "invalid JSON" }, { status: 400 })
  }
  const localItems = Array.isArray(body?.items) ? body.items : []

  const db = getDb()
  const bagRef = db.collection("users").doc(session.user.id).collection("bag")

  try {
    // Read existing cloud items
    const existingSnap = await bagRef.get()
    const cloudItems = existingSnap.docs.map((doc) => doc.data()) as BagItem[]
    const cloudIds = new Set(cloudItems.map((i) => i.id))

    // Anything local that isn't in the cloud is new → batch-write
    const newItems = localItems.filter(
      (i) => i?.id && typeof i.id === "string" && !cloudIds.has(i.id),
    )

    if (newItems.length > 0) {
      const batch = db.batch()
      for (const item of newItems) {
        batch.set(bagRef.doc(item.id), item)
      }
      await batch.commit()
    }

    // Merged set = cloud + newly-written local-only
    const merged = [...cloudItems, ...newItems]
    return NextResponse.json({
      items: merged,
      cloudCount: cloudItems.length,
      localNewCount: newItems.length,
      mergedCount: merged.length,
    })
  } catch (err) {
    console.error("[bag/merge] failed:", err)
    return NextResponse.json({ error: "merge failed" }, { status: 500 })
  }
}
