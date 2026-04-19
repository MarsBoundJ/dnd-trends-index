/*
 * Arcane Analytics — BackerKit run status (Step 12).
 *
 * GET /api/admin/backerkit/status?limit=N
 *   - Verifies admin allowlist (same guard as /run).
 *   - Reads the most recent N BackerKit run docs from Firestore,
 *     ordered by startedAt desc, default limit 10, max 50.
 *   - Returns them shaped for the Harvest Console UI (ISO timestamps
 *     instead of Firestore Timestamps so the client can render
 *     without needing the admin SDK).
 */

import { NextResponse } from "next/server"

import { requireAdmin } from "@/lib/admin-guard"
import { getDb } from "@/lib/firebase-admin"
import type { Timestamp } from "firebase-admin/firestore"

export const dynamic = "force-dynamic"

export interface BackerkitRun {
  runId: string
  type: string
  status: "running" | "completed" | "failed"
  triggeredBy: string
  startedAt: string | null
  finishedAt: string | null
  durationSec: number | null
  summary?: unknown
  error?: string
  stdout?: string
}

function toIso(ts: Timestamp | undefined | null): string | null {
  if (!ts) return null
  try {
    return ts.toDate().toISOString()
  } catch {
    return null
  }
}

export async function GET(req: Request) {
  const admin = await requireAdmin()
  if (!admin) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 })
  }

  const { searchParams } = new URL(req.url)
  const parsed = Number.parseInt(searchParams.get("limit") ?? "10", 10)
  const limit = Number.isFinite(parsed)
    ? Math.max(1, Math.min(parsed, 50))
    : 10

  const db = getDb()
  const snap = await db
    .collection("admin")
    .doc("runs")
    .collection("backerkit")
    .orderBy("startedAt", "desc")
    .limit(limit)
    .get()

  const runs: BackerkitRun[] = snap.docs.map((doc) => {
    const data = doc.data() as {
      runId: string
      type: string
      status: "running" | "completed" | "failed"
      triggeredBy: string
      startedAt?: Timestamp
      finishedAt?: Timestamp
      durationSec?: number
      summary?: unknown
      error?: string
      stdout?: string
    }
    return {
      runId: data.runId,
      type: data.type,
      status: data.status,
      triggeredBy: data.triggeredBy,
      startedAt: toIso(data.startedAt),
      finishedAt: toIso(data.finishedAt),
      durationSec: data.durationSec ?? null,
      summary: data.summary,
      error: data.error,
      stdout: data.stdout,
    }
  })

  return NextResponse.json({ runs })
}
