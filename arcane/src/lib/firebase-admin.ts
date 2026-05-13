/*
 * Trusight — Firestore admin client (Step 11).
 *
 * Thin singleton wrapper around firebase-admin so every server route
 * gets the same initialized instance. The Firebase Admin SDK bypasses
 * Firestore security rules by design, so treat every `getDb()` reader
 * as implicit "I already verified the caller's auth via Auth.js."
 *
 * Credentials (three paths, auto-selected at runtime):
 *   - Vercel (production): GOOGLE_APPLICATION_CREDENTIALS_JSON env var
 *     contains the full service-account JSON as a string. Vercel has no
 *     file system for a JSON path and is AWS-based so no attached GCP
 *     service account exists. The `arcane-web@dnd-trends-index...` SA
 *     key is pasted into Vercel's env-var UI as-is.
 *   - Local dev: Application Default Credentials. Run once per machine:
 *       gcloud auth application-default login
 *     The ADC file lives at
 *       %APPDATA%/gcloud/application_default_credentials.json  (Windows)
 *       $HOME/.config/gcloud/application_default_credentials.json  (mac/linux)
 *   - Cloud Run (future fallback): the attached service account handles
 *     this automatically — no config change required.
 *
 * Hot-reload note: Next 16 Turbopack swaps module instances in dev, which
 * would double-init firebase-admin without the `getApps()` guard below.
 */

import "server-only"

import {
  getApps,
  initializeApp,
  applicationDefault,
  cert,
  type App,
} from "firebase-admin/app"
import { getFirestore, type Firestore } from "firebase-admin/firestore"

const PROJECT_ID = "dnd-trends-index"

let cachedApp: App | undefined

function adminApp(): App {
  if (cachedApp) return cachedApp
  const existing = getApps()[0]
  if (existing) {
    cachedApp = existing
    return existing
  }

  // Vercel path when GOOGLE_APPLICATION_CREDENTIALS_JSON is set; falls back
  // to applicationDefault() for local dev (gcloud ADC) and Cloud Run
  // (attached service account). See header comment for the full matrix.
  const jsonCreds = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON
  const credential = jsonCreds
    ? cert(JSON.parse(jsonCreds))
    : applicationDefault()

  cachedApp = initializeApp({
    credential,
    projectId: PROJECT_ID,
  })
  return cachedApp
}

/**
 * Returns the project's default Firestore database. Call this lazily
 * from inside route handlers — never at module top-level — so a missing
 * ADC doesn't crash the dev server on boot.
 */
export function getDb(): Firestore {
  return getFirestore(adminApp())
}
