/*
 * Trusight — BackerKit harvest trigger (Step 12 + self-report fix).
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

  // AWAIT the trigger. The old code fired this fetch fire-and-forget
  // and returned immediately — but on Vercel the lambda is frozen the
  // instant the response flushes, which could kill the un-awaited
  // fetch BEFORE it was even sent. The harvester then never ran for
  // this runId, the doc sat "running", and 5 min later the /status
  // stale-detector mislabeled it "harvester may have crashed before
  // writing status" (misleading: it was never triggered at all).
  //
  // Awaiting keeps the lambda alive through dispatch. The BackerKit
  // harvest is fast (~2-4s) and maxDuration is 15s, so in the normal
  // case the fetch even resolves with the harvester's result before
  // we return. AbortSignal.timeout caps the wait safely under
  // maxDuration. The harvester still self-reports terminal status to
  // admin/runs/backerkit/{runId} (see backerkit_harvester/main.py);
  // the /status stale-detector remains the final backstop.
  try {
    await fetch(BACKERKIT_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_id: runId }),
      signal: AbortSignal.timeout(12_000),
    })
    // Resolved (any HTTP status) → trigger delivered and the harvester
    // ran; it self-reports the terminal status to the same doc.
  } catch (err) {
    const name = err instanceof Error ? err.name : ""
    if (name === "TimeoutError" || name === "AbortError") {
      // Request WAS dispatched (connection opened, Cloud Run is
      // processing) — we just didn't wait for the full harvest. The
      // self-report closes the loop; stale-detector is the backstop.
      // NOT a failure — do not mark the run failed here.
      console.warn(
        "[backerkit/run] trigger dispatched; harvest still running at 12s cutoff — self-report will close the loop",
      )
    } else {
      // Genuine dispatch failure (DNS / connection refused) — the
      // harvester never received the request. Mark it failed now;
      // we awaited, so the lambda is still alive to write this.
      console.error("[backerkit/run] trigger dispatch failed:", err)
      try {
        await runRef.update({
          status: "failed",
          finishedAt: FieldValue.serverTimestamp(),
          error: `trigger dispatch failed: ${err instanceof Error ? err.message : String(err)}`,
        })
      } catch {
        /* swallow — best effort */
      }
    }
  }

  return NextResponse.json({ runId, status: "running" })
}
