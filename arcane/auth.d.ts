/*
 * Arcane Analytics — Auth.js v5 type augmentation (Step 11).
 *
 * Adds a stable `uid` field to the JWT and `user.id` on the session so
 * TypeScript knows about the custom fields we set in auth.ts callbacks.
 */

import "next-auth"
import "next-auth/jwt"

declare module "next-auth" {
  interface Session {
    user: {
      id: string
      name?: string | null
      email?: string | null
      image?: string | null
    }
  }
}

declare module "next-auth/jwt" {
  interface JWT {
    uid?: string
  }
}
