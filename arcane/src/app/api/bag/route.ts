/*
 * Trusight — Bag of Holding API (Step 11).
 *
 * Server-only CRUD for a signed-in user's Bag of Holding items. Each
 * item is a single Firestore document at `users/{uid}/bag/{id}` where
 * `id` is the client-side item id (same one the Zustand store uses).
 *
 * All handlers read the session via `auth()` and 401 if not signed in.
 * Anonymous bag use is still supported client-side via localStorage —
 * unauthenticated callers never reach this route.
 */

import { NextResponse } from "next/server"
import { auth } from "../../../../auth"
import { getDb } from "@/lib/firebase-admin"

export const dynamic = "force-dynamic"

// StowedItem shape lifted from bag-store.ts. Kept loose here — the store
// is the source of truth for the discriminated union; the API just
// round-trips whatever shape the client sent.
interface BagItem {
  kind: "card" | "sage-answer" | "sage-thread"
  id: string
  [key: string]: unknown
}

function bagRef(uid: string) {
  return getDb().collection("users").doc(uid).collection("bag")
}

// ─── GET: list all items for the signed-in user ────────────────────────────
export async function GET() {
  const session = await auth()
  if (!session?.user?.id) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 })
  }

  try {
    const snap = await bagRef(session.user.id).get()
    const items = snap.docs.map((doc) => doc.data()) as BagItem[]
    return NextResponse.json({ items })
  } catch (err) {
    console.error("[bag] GET failed:", err)
    return NextResponse.json({ error: "read failed" }, { status: 500 })
  }
}

// ─── POST: upsert one item ─────────────────────────────────────────────────
// Body: a single BagItem. We use its `id` as the Firestore doc id so
// re-stowing the same concept is idempotent.
export async function POST(req: Request) {
  const session = await auth()
  if (!session?.user?.id) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 })
  }

  let item: BagItem
  try {
    item = (await req.json()) as BagItem
  } catch {
    return NextResponse.json({ error: "invalid JSON" }, { status: 400 })
  }
  if (!item?.id || typeof item.id !== "string") {
    return NextResponse.json({ error: "id required" }, { status: 400 })
  }
  if (!item.kind || !["card", "sage-answer", "sage-thread"].includes(item.kind)) {
    return NextResponse.json({ error: "kind required" }, { status: 400 })
  }

  try {
    await bagRef(session.user.id).doc(item.id).set(item)
    return NextResponse.json({ ok: true, id: item.id })
  } catch (err) {
    console.error("[bag] POST failed:", err)
    return NextResponse.json({ error: "write failed" }, { status: 500 })
  }
}

// ─── DELETE: remove one item by ?id=... ────────────────────────────────────
export async function DELETE(req: Request) {
  const session = await auth()
  if (!session?.user?.id) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 })
  }

  const { searchParams } = new URL(req.url)
  const id = searchParams.get("id")?.trim()
  if (!id) {
    return NextResponse.json({ error: "id required" }, { status: 400 })
  }

  try {
    await bagRef(session.user.id).doc(id).delete()
    return NextResponse.json({ ok: true, id })
  } catch (err) {
    console.error("[bag] DELETE failed:", err)
    return NextResponse.json({ error: "delete failed" }, { status: 500 })
  }
}
