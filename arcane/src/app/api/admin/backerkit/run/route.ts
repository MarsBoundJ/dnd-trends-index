/*
 * Arcane Analytics — BackerKit harvest trigger (Step 12).
 *
 * POST /api/admin/backerkit/run
 *   - Verifies caller is on the admin allowlist (belt-and-suspenders;
 *     the proxy.ts also guards /admin/* pages but API routes sit
 *     outside that matcher).
 *   - Writes a Firestore run-log doc at admin/runs/{runId} with
 *     status "queued", immediately flipped to "running".
 *   - Fires the backerkit-harvester Cloud Function (fire-and-forget).
 *   - When that fetch resolves (in the background), updates the run
 *     doc with status "completed" / "failed" + summary / error.
 *   - Returns { runId } to the client immediately so the UI can start
 *     polling /status without blocking.
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

const BACKERKIT_URL =
  "https://backerkit-harvester-kfh5mgjgiq-uc.a.run.app"

export async function POST() {
  const admin = await requireAdmin()
  if (!admin) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 })
  }

  const db = getDb()
  const runId = randomUUID()
  const runRef = db.collection("admin").doc("runs").collection("backerkit").doc(runId)

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

  // Fire-and-forget. Node's fetch() returns a Promise we deliberately
  // don't await — we want to return `{ runId }` to the client now and
  // have the harvester finish on its own schedule. We DO attach
  // .then/.catch so we can flip status when it settles.
  //
  // `maxDuration` is bumped to 15s above only so the initial fetch
  // handshake has room; the Cloud Function's actual work can take
  // minutes and will outlive this route handler's lambda. That's fine
  // on Cloud Run / Vercel Functions — the fetch stays alive inside
  // the already-returned response's completion callbacks.
  void (async () => {
    const startedAtMs = Date.now()
    try {
      // Use POST with an explicit empty-JSON body so Content-Length is set.
      // Cloud Run's HTTP frontend rejects body-less POSTs with 411 Length
      // Required — a `fetch(url, { method: "POST" })` with no body omits
      // the header. The backerkit-harvester function reads no fields from
      // the body, so the value is irrelevant — it just needs to be there.
      const res = await fetch(BACKERKIT_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      })
      const finishedAtMs = Date.now()
      const durationSec = Math.round((finishedAtMs - startedAtMs) / 1000)

      if (!res.ok) {
        const text = await res.text().catch(() => "")
        await runRef.update({
          status: "failed",
          finishedAt: FieldValue.serverTimestamp(),
          durationSec,
          error: `HTTP ${res.status} ${res.statusText}`,
          stdout: text.slice(0, 4000),
        })
        return
      }

      // The harvester returns a JSON summary on success.
      let summary: unknown = null
      try {
        summary = await res.json()
      } catch {
        summary = await res.text().catch(() => "")
      }

      await runRef.update({
        status: "completed",
        finishedAt: FieldValue.serverTimestamp(),
        durationSec,
        summary,
      })
    } catch (err) {
      const finishedAtMs = Date.now()
      const durationSec = Math.round((finishedAtMs - startedAtMs) / 1000)
      await runRef
        .update({
          status: "failed",
          finishedAt: FieldValue.serverTimestamp(),
          durationSec,
          error: err instanceof Error ? err.message : String(err),
        })
        .catch((updateErr) =>
          console.error("[backerkit/run] update after failure failed:", updateErr),
        )
    }
  })()

  return NextResponse.json({ runId, status: "running" })
}
