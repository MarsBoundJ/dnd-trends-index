"use client"

/*
 * Arcane Analytics — landing page sign-in CTA (Step 14 landing polish).
 *
 * Prominent "Sign in" button for the unauthenticated root landing page.
 * Larger and more visually weighted than the SignInMenu pill in the
 * site header — the landing page has one job (get the prospect into
 * the sign-in flow) and the CTA should own that visual attention.
 *
 * On click: routes to Auth.js's built-in sign-in page (same as
 * SignInMenu does), passing `/overview` as the post-sign-in landing
 * so new users go straight to the product rather than back to this
 * marketing page.
 */

import { signIn } from "next-auth/react"
import { LogIn } from "lucide-react"

import { cn } from "@/lib/utils"

export function LandingCta() {
  return (
    <button
      type="button"
      onClick={() => signIn(undefined, { callbackUrl: "/overview" })}
      aria-label="Sign in to Arcane Analytics"
      className={cn(
        "inline-flex items-center gap-2.5 rounded-lg px-6 py-3",
        "border border-ember bg-ember/15",
        "font-sans text-base font-medium text-parchment",
        "transition-colors hover:bg-ember/25 hover:border-ember-bright",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian",
      )}
    >
      <LogIn className="h-4 w-4 text-ember-bright" aria-hidden />
      <span>Sign in</span>
    </button>
  )
}
