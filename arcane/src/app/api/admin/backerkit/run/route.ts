/*
 * Arcane Analytics — BackerKit harvest trigger (Step 12 + self-report fix).
 *
 * POST /api/admin/backerkit/run
 *   - Verifies caller is on the admin allowlist (belt-and-suspenders;
 *     the proxy.ts also guards /admin/* pages but API routes sit
 *     outside that matcher).
 *   - Writes a Firestore run-log doc at admin/runs/backerkit/{runId}
 *     with status "running".
 *   - Fires the backerkit-harvester Cloud Function with the runId in
 *     the request body. The harvester SELF-REPORTS completion back to
 *     the same Firestore doc when done (see
 *     cloud_functions/backerkit_harvester/main.py). That removes our
 *     dependency on the Next.js route staying alive to update status —
 *     important because Vercel serverless terminates the function as
 *     soon as the response is sent, so any background IIFE awaiting
 *     the harvester's full response would be killed mid-flight.
 *   - Returns { runId } to the client immediately. UI polls /status
 *     which reads Firestore; the harvester's self-report closes the
 *     loop.
 *
 * Pre-self-report (Apr 19 runs) the fire-and-forget IIFE pattern
 * DID work because local dev keeps the Node process alive until all
 * promises settle. On Vercel (post-Apr 23 deploy), background
 * promises are dropped the moment the response flushes — any run
 * from the admin console hung in "running" state forever until the
 * harvester was upgraded to self-report.
 *
 * The BackerKit function is public (--allow-unauthenticated, per the
 * repo's convention). The admin-gate on this route is the trust
 * boundary — we accepted not pushing a shared-secret header through
 * to the Cloud Function for Step 12.
 */

import { NextResponse } from "next/server"
import { randomUUID } from "node:crypto"

import { requireAdmin } from "@/lib/admin-guard"
import { getDb } from "@/lib/firebase-admin"
import { FieldValue } from "firebase-admin/firestore"

export const dynamic = "force-dynamic"
export const maxDuration = 15

const BACKERKIT_URL = "https://backerkit-harvester-kfh5mgjgiq-uc.a.run.app"

export async function POST() {
  const admin = await requireAdmin()
  if (!admin) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 })
  }

  const db = getDb()
  const runId = randomUUID()
  const runRef = db
    .collection("admin")
    .doc("runs")
    .collection("backerkit")
    .doc(runId)

  try {
    await runRef.set({
      runId,
      type: "backerkit",
      status: "running",
      triggeredBy: admin.email,
      startedAt: FieldValue.serverTimestamp(),
    })
  } catch (err) {
    console.error("[backerkit/run] Firestore write failed:", err)
    return NextResponse.json({ error: "run log init failed" }, { status: 500 })
  }

  // Fire-and-forget trigger. We attach `.catch()` so unhandled promise
  // rejections don't spam Vercel's error reporter, but otherwise we
  // don't need to await — the harvester self-reports completion to
  // Firestore directly.
  //
  // Cloud Run receives the request within ~50-200ms of dispatch, so
  // even if Vercel tears down our function immediately after the
  // response, the trigger has already landed. The status-tracking
  // responsibility now lives with the harvester; our job is only to
  // initiate + log.
  //
  // The harvester reads `run_id` from its request body and writes
  // its own status updates to admin/runs/backerkit/{runId}. See
  // cloud_functions/backerkit_harvester/main.py::_report_run_status.
  fetch(BACKERKIT_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ run_id: runId }),
  }).catch((err) => {
    console.error("[backerkit/run] trigger dispatch failed:", err)
    // Best-effort: try to mark the run as failed so the UI doesn't
    // spin forever. This itself is fire-and-forget — if Vercel has
    // already killed us, the /status route's stale-run detector
    // (defense in depth) will eventually heal it.
    runRef
      .update({
        status: "failed",
        finishedAt: FieldValue.serverTimestamp(),
        error: `trigger dispatch failed: ${err instanceof Error ? err.message : String(err)}`,
      })
      .catch(() => {
        /* swallow — dead lambda, nothing we can do */
      })
  })

  return NextResponse.json({ runId, status: "running" })
}
