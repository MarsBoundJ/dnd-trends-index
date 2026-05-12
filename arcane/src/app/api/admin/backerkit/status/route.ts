/*
 * Trusight — BackerKit run status (Step 12 + stale-run detector).
 *
 * GET /api/admin/backerkit/status?limit=N
 *   - Verifies admin allowlist (same guard as /run).
 *   - Reads the most recent N BackerKit run docs from Firestore,
 *     ordered by startedAt desc, default limit 10, max 50.
 *   - **Stale-run detection (Apr 24):** before returning, any doc
 *     still in status="running" with startedAt older than 5 minutes
 *     is auto-patched to status="failed" with a descriptive error.
 *     This is defense-in-depth against the harvester's self-report
 *     mechanism failing (network hiccup, Cloud Run crash before the
 *     Firestore write, or a legacy run triggered without a run_id).
 *   - Returns the runs shaped for the Harvest Console UI (ISO
 *     timestamps, Firestore Timestamps pre-converted).
 *
 * The BackerKit harvester itself now self-reports completion to the
 * same Firestore doc (see cloud_functions/backerkit_harvester/main.py).
 * So in the happy path, no stale detection is needed — this route is
 * purely a cleanup safety net.
 */

import { NextResponse } from "next/server"

import { requireAdmin } from "@/lib/admin-guard"
import { getDb } from "@/lib/firebase-admin"
import { FieldValue, type Timestamp } from "firebase-admin/firestore"

export const dynamic = "force-dynamic"

/**
 * A run is considered stale if it's still in "running" state and its
 * startedAt is older than this threshold. The BackerKit harvester
 * normally takes 2-4 seconds to complete, so 5 minutes is generous —
 * any run stuck that long almost certainly means the self-report
 * didn't land, not that the harvester is still working.
 */
const STALE_AFTER_MS = 5 * 60 * 1000

export interface BackerkitRun {
  runId: string
  type: string
  status: "running" | "completed" | "failed" | "stalled"
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

  const now = Date.now()
  const staleRunIds: string[] = []

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

    const startedAtIso = toIso(data.startedAt)
    const startedAtMs = data.startedAt?.toMillis?.() ?? 0

    // Stale-run detection: any "running" entry whose startedAt is
    // older than STALE_AFTER_MS gets flagged. We'll patch Firestore
    // in a batch after the read loop (to avoid blocking the response
    // on N individual writes).
    const isStale =
      data.status === "running" &&
      startedAtMs > 0 &&
      now - startedAtMs > STALE_AFTER_MS

    if (isStale) {
      staleRunIds.push(data.runId)
    }

    return {
      runId: data.runId,
      type: data.type,
      status: isStale ? "failed" : data.status,
      triggeredBy: data.triggeredBy,
      startedAt: startedAtIso,
      finishedAt: isStale ? new Date().toISOString() : toIso(data.finishedAt),
      durationSec: isStale
        ? Math.round((now - startedAtMs) / 1000)
        : data.durationSec ?? null,
      summary: data.summary,
      error: isStale
        ? "Self-report not received within 5 minutes — harvester may have crashed before writing status. Check Cloud Run logs for actual outcome."
        : data.error,
      stdout: data.stdout,
    }
  })

  // Fire-and-forget the Firestore patches for stale runs. This route
  // returns the patched response regardless of whether these writes
  // succeed — idempotent, eventually consistent. Re-running the same
  // GET would attempt the patches again, so a transient Firestore
  // error on this write is self-healing on the next poll.
  if (staleRunIds.length > 0) {
    const collection = db.collection("admin").doc("runs").collection("backerkit")
    const nowTimestamp = FieldValue.serverTimestamp()
    void Promise.allSettled(
      staleRunIds.map((runId) =>
        collection.doc(runId).update({
          status: "failed",
          finishedAt: nowTimestamp,
          error:
            "Self-report not received within 5 minutes — harvester may have crashed before writing status. Check Cloud Run logs for actual outcome.",
        }),
      ),
    ).then((results) => {
      const failed = results.filter((r) => r.status === "rejected").length
      if (failed > 0) {
        console.warn(
          `[backerkit/status] Stale-run patch: ${failed}/${staleRunIds.length} updates failed (will retry on next poll)`,
        )
      } else {
        console.log(
          `[backerkit/status] Stale-run patch: healed ${staleRunIds.length} stuck run(s)`,
        )
      }
    })
  }

  return NextResponse.json({ runs })
}
