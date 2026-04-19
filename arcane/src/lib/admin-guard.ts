/*
 * Arcane Analytics — Admin guard helpers (Step 12).
 *
 * Thin wrapper over `auth()` + ADMIN_EMAILS check so every admin-only
 * route handler doesn't have to re-implement the allowlist logic. The
 * proxy.ts already enforces this at the page-navigation level, but
 * route handlers under /api/admin/* aren't matched by the proxy (API
 * routes are excluded from the proxy matcher by design to keep the
 * proxy focused on server-rendered pages). So every /api/admin/* route
 * re-checks here.
 */

import "server-only"
import { auth } from "../../auth"

export interface AdminSession {
  email: string
  uid: string
  name?: string | null
}

/**
 * Returns the signed-in admin's identity if they're on the allowlist,
 * otherwise returns null. Callers return 401 / 403 responses themselves
 * so HTTP semantics stay explicit.
 */
export async function requireAdmin(): Promise<AdminSession | null> {
  const session = await auth()
  const email = session?.user?.email?.toLowerCase()
  if (!email) return null

  const allowlist = new Set(
    (process.env.ADMIN_EMAILS ?? "")
      .split(",")
      .map((e) => e.trim().toLowerCase())
      .filter(Boolean),
  )
  if (!allowlist.has(email)) return null

  return {
    email,
    uid: session?.user?.id ?? email,
    name: session?.user?.name ?? null,
  }
}
