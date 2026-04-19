/*
 * Arcane Analytics — Auth.js v5 full config (Steps 11 + 11.5).
 *
 * Imports the shared edge-safe `authConfig` from auth.config.ts and
 * layers in the Firebase adapter — a Node-only dependency that the
 * proxy (which runs in the edge runtime) must never see. Auth.js v5
 * documents this split as the canonical pattern for "middleware +
 * database adapter."
 *
 * The adapter persists Users / Accounts / VerificationTokens to
 * Firestore. Collection names are renamed from the default `users`
 * etc. to the `authjs_*` namespace so they don't collide with our
 * existing Bag-of-Holding user docs at `users/{uid}/bag/...`.
 *
 * Secrets in .env.local (gitignored):
 *   AUTH_SECRET          random 32+ bytes, base64. Signs JWTs.
 *   AUTH_GOOGLE_ID       OAuth client ID (GCP console).
 *   AUTH_GOOGLE_SECRET   OAuth client secret (GCP console).
 *   AUTH_RESEND_KEY      Resend API key (resend.com dashboard).
 */

import NextAuth from "next-auth"
import Resend from "next-auth/providers/resend"
import { FirestoreAdapter } from "@auth/firebase-adapter"

import { authConfig } from "./auth.config"
import { getDb } from "@/lib/firebase-admin"

export const { handlers, auth, signIn, signOut } = NextAuth({
  ...authConfig,
  // Add the Resend email provider here (NOT in auth.config.ts) —
  // magic-link sign-in requires a database adapter, which would
  // break proxy.ts's edge-runtime `NextAuth(authConfig)` if it were
  // in the base config.
  providers: [
    ...authConfig.providers,
    Resend({
      apiKey: process.env.AUTH_RESEND_KEY,
      // Resend's sandbox-from-address. In sandbox mode Resend only
      // delivers mail to the account owner's own verified email
      // (halftonejones@gmail.com here). Good enough for Step 11.5 —
      // once a custom sending domain is verified at resend.com,
      // swap this for something like "sage@arcaneanalytics.com".
      from: "onboarding@resend.dev",
    }),
  ],
  // Reuse our existing firebase-admin singleton rather than letting
  // the adapter stand up a second Firebase App. Keeps initialization
  // centralized in src/lib/firebase-admin.ts.
  adapter: FirestoreAdapter({
    firestore: getDb(),
    collections: {
      users: "authjs_users",
      accounts: "authjs_accounts",
      sessions: "authjs_sessions",
      verificationTokens: "authjs_verification_tokens",
    },
  }),
})
