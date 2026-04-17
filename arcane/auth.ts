/*
 * Arcane Analytics — Auth.js v5 (NextAuth) config (Step 11).
 *
 * Lives at the project root because Auth.js v5 expects to find auth.ts
 * in either the project root or `src/auth.ts`. We pick root so the
 * Next.js App Router's path aliases (`@/...`) stay clean — auth.ts itself
 * is a plain TS config, no React, no aliases.
 *
 * Step 11 scope: Google OAuth only. Magic link provider is deferred to
 * a Step 11.5 follow-up (project_frontend_build_status.md memory tracks
 * the decision). Session strategy is stateless JWT — no database
 * adapter is needed in v1. When we need persistent session rows (e.g.
 * for revocation at scale or long-lived sessions), we can swap in
 * @auth/firebase-adapter without changing the route handler.
 *
 * Secrets in .env.local (gitignored):
 *   AUTH_SECRET          random 32+ bytes, base64. Signs JWTs.
 *   AUTH_GOOGLE_ID       OAuth client ID (GCP console).
 *   AUTH_GOOGLE_SECRET   OAuth client secret (GCP console).
 */

import NextAuth from "next-auth"
import Google from "next-auth/providers/google"

export const { handlers, auth, signIn, signOut } = NextAuth({
  providers: [
    Google({
      clientId: process.env.AUTH_GOOGLE_ID,
      clientSecret: process.env.AUTH_GOOGLE_SECRET,
    }),
  ],
  // JWT session — stateless, no DB needed for Step 11.
  session: { strategy: "jwt" },
  callbacks: {
    async jwt({ token, account, profile }) {
      // On initial sign-in, stash Google's stable `sub` (user ID) on the
      // token. We'll use it as the `users/{uid}/bag/...` Firestore path
      // when Step 11 (d) lands.
      if (account && profile?.sub) {
        token.uid = profile.sub
      }
      return token
    },
    async session({ session, token }) {
      // Expose uid on the session object so client + server components can
      // read it via `const { user } = await auth(); user.id`.
      if (token.uid && session.user) {
        session.user.id = token.uid as string
      }
      return session
    },
  },
})
