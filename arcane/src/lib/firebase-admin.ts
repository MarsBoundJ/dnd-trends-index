/*
 * Arcane Analytics — Firestore admin client (Step 11).
 *
 * Thin singleton wrapper around firebase-admin so every server route
 * gets the same initialized instance. The Firebase Admin SDK bypasses
 * Firestore security rules by design, so treat every `getDb()` reader
 * as implicit "I already verified the caller's auth via Auth.js."
 *
 * Credentials:
 *   - Local dev: Application Default Credentials. Run once per machine:
 *       gcloud auth application-default login
 *     The ADC file lives at
 *       %APPDATA%/gcloud/application_default_credentials.json  (Windows)
 *       $HOME/.config/gcloud/application_default_credentials.json  (mac/linux)
 *   - Cloud Run (future): the attached service account handles this
 *     automatically — no config change required.
 *
 * Hot-reload note: Next 16 Turbopack swaps module instances in dev, which
 * would double-init firebase-admin without the `getApps()` guard below.
 */

import "server-only"

import {
  getApps,
  initializeApp,
  applicationDefault,
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
  cachedApp = initializeApp({
    credential: applicationDefault(),
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
