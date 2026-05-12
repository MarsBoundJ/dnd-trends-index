/*
 * Trusight — Auth.js v5 route handler (Step 11).
 *
 * Re-exports the GET/POST handlers from the top-level auth.ts. Every
 * Auth.js request (sign-in, callback, session fetch, sign-out) routes
 * through /api/auth/[...nextauth]/...
 */

import { handlers } from "../../../../../auth"

export const { GET, POST } = handlers
