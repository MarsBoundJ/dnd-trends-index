/*
 * Arcane Analytics — Auth.js v5 base config (Step 11.5).
 *
 * Edge-compatible. Shared between:
 *   - auth.ts        (Node runtime; adds the Firebase adapter)
 *   - src/proxy.ts   (Edge runtime; imports only this file)
 *
 * Anything Node-only (firebase-admin, fs, etc.) MUST NOT be imported
 * at the top level of this file. Auth.js v5 documents this split as
 * the canonical pattern for "middleware + database adapter."
 *
 * Providers:
 *   1. Google OAuth (Step 11) — primary sign-in.
 *   2. Resend magic-link (Step 11.5) — email passwordless. Requires
 *      the Firebase adapter in auth.ts to persist verification tokens.
 */

import type { NextAuthConfig } from "next-auth"
import Google from "next-auth/providers/google"

// Base config contains ONLY providers that work without a database
// adapter — Google OAuth, here. The Resend magic-link provider
// requires an adapter to persist verification tokens and is added in
// the full auth.ts layer (which runs in the Node runtime). If Resend
// were added here, proxy.ts's edge-runtime `NextAuth(authConfig)`
// would throw MissingAdapter on startup.
export const authConfig: NextAuthConfig = {
  providers: [
    Google({
      clientId: process.env.AUTH_GOOGLE_ID,
      clientSecret: process.env.AUTH_GOOGLE_SECRET,
      // Allow Google to link to an existing user record created via
      // another provider (e.g. a Resend magic-link account on the same
      // email). Safe because Google verifies email ownership before
      // issuing the OAuth profile, and Resend verifies ownership by
      // requiring the user to click a link from that inbox — both
      // arrive at "user controls this email" with genuine proof, so
      // treating them as the same account is not a takeover vector.
      allowDangerousEmailAccountLinking: true,
    }),
  ],
  session: { strategy: "jwt" },
  callbacks: {
    async jwt({ token, user, account, profile }) {
      // Establish a stable `token.uid` the rest of the app can key on.
      // Priority order:
      //   1. Google OAuth profile.sub — the historical path used by
      //      Step 11's Bag of Holding. Keeps existing OAuth users
      //      on the same Firestore bag path across the adapter switch.
      //   2. Adapter-generated user.id — used for magic-link sign-ins,
      //      where there's no OAuth profile. A magic-link user gets
      //      their own Firestore bag keyed by this id; if they later
      //      add Google to the same email, the adapter links the
      //      accounts and subsequent sign-ins will still resolve here.
      //
      // Also: Resend magic-link user records only have `email`. When
      // Google later links to the same email via
      // allowDangerousEmailAccountLinking, it DOESN'T update the
      // user record — so falling back to user.name / user.image would
      // leave the session nameless and avatar-less. We prefer the
      // OAuth profile fields here on every sign-in, which keeps
      // sessions fully populated.
      if (account && profile) {
        if (typeof profile.sub === "string") {
          token.uid = profile.sub
        }
        if (typeof profile.name === "string") {
          token.name = profile.name
        }
        if (typeof profile.email === "string") {
          token.email = profile.email
        }
        if (typeof profile.picture === "string") {
          token.picture = profile.picture
        }
      } else if (user?.id) {
        if (!token.uid) token.uid = user.id
        if (user.name && !token.name) token.name = user.name
        if (user.email && !token.email) token.email = user.email
        if (user.image && !token.picture) token.picture = user.image
      }
      return token
    },
    async session({ session, token }) {
      if (token.uid && session.user) {
        session.user.id = token.uid as string
      }
      return session
    },
  },
}
