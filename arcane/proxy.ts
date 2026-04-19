/*
 * Arcane Analytics — Admin gate (Step 12).
 *
 * Next 16 Proxy (formerly Middleware — file convention was renamed; see
 * `node_modules/next/dist/docs/01-app/03-api-reference/03-file-conventions/proxy.md`).
 * Runs before `/admin/*` routes render and enforces the email allowlist.
 *
 * This is the PRE-deploy gate: it keeps /admin/* reachable only by users
 * whose session email appears in ADMIN_EMAILS. IAP (Google Cloud Identity-
 * Aware Proxy) would be a *second* layer in front of the Cloud Run service
 * once we deploy — at which point this proxy becomes belt-and-suspenders.
 *
 * Auth.js v5 exports an `auth()` higher-order function that resolves the
 * session and exposes it on `req.auth`. We use that here instead of
 * fetching the session manually — it's the supported pattern for
 * Auth.js + Next proxy.
 */

import { auth } from "./auth"
import { NextResponse } from "next/server"

/**
 * Comma-separated allowlist. Kept server-only. Mirrored to
 * NEXT_PUBLIC_ADMIN_EMAILS for the Atlas's conditional Admin tile —
 * keep both in sync when adding/removing admins.
 */
const ADMIN_EMAILS = new Set(
  (process.env.ADMIN_EMAILS ?? "")
    .split(",")
    .map((e) => e.trim().toLowerCase())
    .filter(Boolean),
)

export default auth((req) => {
  const email = req.auth?.user?.email?.toLowerCase()

  // Not signed in → bounce to Auth.js sign-in with a callbackUrl back here
  // so a successful sign-in lands them where they tried to go.
  if (!email) {
    const signInUrl = new URL("/api/auth/signin", req.nextUrl.origin)
    signInUrl.searchParams.set("callbackUrl", req.nextUrl.pathname + req.nextUrl.search)
    return NextResponse.redirect(signInUrl)
  }

  // Signed in but not an admin → friendly 403 page.
  if (!ADMIN_EMAILS.has(email)) {
    return NextResponse.redirect(new URL("/not-authorized", req.nextUrl.origin))
  }

  // Admin — let the request continue to the page.
  return NextResponse.next()
})

export const config = {
  // Only run on /admin/*. Other routes skip this proxy entirely.
  matcher: ["/admin/:path*"],
}
