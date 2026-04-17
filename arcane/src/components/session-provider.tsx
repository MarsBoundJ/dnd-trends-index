"use client"

/*
 * Arcane Analytics — SessionProvider wrapper (Step 11).
 *
 * Thin "use client" boundary around Auth.js's SessionProvider so the
 * root layout stays a Server Component. Every client component below
 * this boundary can then use `useSession()` without hitting the
 * "SessionProvider must be rendered in a Client Component" error.
 */

import { SessionProvider } from "next-auth/react"

export function AuthSessionProvider({
  children,
}: {
  children: React.ReactNode
}) {
  return <SessionProvider>{children}</SessionProvider>
}
