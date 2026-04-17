"use client"

/*
 * Arcane Analytics — sign-in menu (Step 11).
 *
 * Lives in SiteHeader, just left of the Atlas trigger.
 *
 * Two states:
 *   1. Unauthenticated → "Sign in" pill button kicking off Google OAuth.
 *   2. Authenticated   → avatar + name (hidden on narrow) opening a small
 *                        popover with email + Sign out.
 *
 * Loading state is rendered as an inert placeholder pill so the header
 * layout doesn't reflow between server HTML and client hydration when
 * the session resolves.
 *
 * All Auth.js surface area (signIn / signOut / useSession) lives in this
 * one file so other components don't have to import from next-auth/react
 * directly.
 */

import Image from "next/image"
import { signIn, signOut, useSession } from "next-auth/react"
import { LogIn, LogOut, User } from "lucide-react"

import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { cn } from "@/lib/utils"

export function SignInMenu() {
  const { data: session, status } = useSession()

  if (status === "loading") {
    return <SignedOutButton loading />
  }

  if (!session?.user) {
    return <SignedOutButton />
  }

  return <SignedInMenu user={session.user} />
}

// ─── Signed-out button ──────────────────────────────────────────────────────

function SignedOutButton({ loading = false }: { loading?: boolean }) {
  return (
    <button
      type="button"
      disabled={loading}
      // Trigger Google OAuth via Auth.js. `callbackUrl` lands the user
      // back on the current page rather than the default /api/auth/signin.
      onClick={() => signIn("google", { callbackUrl: window.location.href })}
      aria-label="Sign in with Google"
      className={cn(
        "inline-flex items-center gap-2 rounded-md px-3 py-1.5",
        "border border-bronze/60 bg-onyx/60",
        "text-sm text-parchment",
        "transition-colors hover:border-ember hover:bg-iron",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-iron",
        loading && "opacity-60 cursor-wait",
      )}
    >
      <LogIn className="h-4 w-4 text-ember-bright" aria-hidden />
      <span className="hidden sm:inline">Sign in</span>
    </button>
  )
}

// ─── Signed-in menu ─────────────────────────────────────────────────────────

// Shape loose on input to play nicely with Auth.js v5's own User type
// (id + all fields are nullable/optional in the base type). We don't
// actually need id in this component — it's consumed by the bag API
// route via the server-side `auth()` helper, not passed through props.
interface SessionUser {
  name?: string | null
  email?: string | null
  image?: string | null
}

function SignedInMenu({ user }: { user: SessionUser }) {
  const displayName = user.name ?? user.email ?? "Signed in"

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`Account menu for ${displayName}`}
          className={cn(
            "inline-flex items-center gap-2 rounded-full md:rounded-md",
            "md:px-2.5 md:py-1 md:border md:border-bronze/60 md:bg-onyx/60",
            "text-sm text-parchment",
            "transition-colors md:hover:border-ember md:hover:bg-iron",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-iron",
          )}
        >
          {user.image ? (
            <Image
              src={user.image}
              alt=""
              width={24}
              height={24}
              className="h-6 w-6 rounded-full border border-bronze/60"
            />
          ) : (
            <span className="inline-flex h-6 w-6 items-center justify-center rounded-full border border-bronze/60 bg-iron">
              <User className="h-3.5 w-3.5 text-ember-bright" aria-hidden />
            </span>
          )}
          <span className="hidden md:inline max-w-[120px] truncate">
            {user.name?.split(" ")[0] ?? "Account"}
          </span>
        </button>
      </PopoverTrigger>
      <PopoverContent
        side="bottom"
        align="end"
        className="w-64 bg-iron border border-bronze text-parchment p-3 space-y-3"
      >
        <div className="space-y-0.5">
          <p className="font-display text-sm font-semibold truncate">
            {user.name ?? "Signed in"}
          </p>
          {user.email && (
            <p className="text-xs text-ash truncate">{user.email}</p>
          )}
        </div>
        <button
          type="button"
          onClick={() => signOut({ callbackUrl: "/" })}
          className={cn(
            "w-full inline-flex items-center justify-center gap-2 rounded-md px-3 py-1.5",
            "border border-bronze/60 bg-onyx text-sm text-parchment",
            "transition-colors hover:border-ember hover:bg-iron",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-iron",
          )}
        >
          <LogOut className="h-3.5 w-3.5" aria-hidden />
          Sign out
        </button>
      </PopoverContent>
    </Popover>
  )
}
