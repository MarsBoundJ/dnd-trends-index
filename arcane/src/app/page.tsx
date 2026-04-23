import type { Metadata } from "next"
import { redirect } from "next/navigation"

import { auth } from "../../auth"
import { LandingCta } from "@/components/landing-cta"

/*
 * Arcane Analytics — root landing page.
 *
 * Two distinct experiences driven by authentication state:
 *
 *   - Authenticated user → immediately redirected to /overview. No
 *     point showing marketing copy to someone already inside the
 *     product. Session is read server-side via auth() so the redirect
 *     happens at the edge / RSC layer, not after a client-side flash.
 *
 *   - Unauthenticated visitor → marketing landing with a single
 *     prominent Sign in CTA. This is what a WotC / Hasbro prospect
 *     sees when they click the link in outreach email. Copy is
 *     deliberately minimal: the email did the selling, the landing
 *     page's job is to look legitimate and route them into sign-in.
 *
 * Copy matches the canonical outreach language from
 * project_hasbro_pitch_problems_solutions.md §"What Arcane IS" — the
 * 10-K + live-signal duality, the Universes Beyond Matrix, the
 * eight-voice Council.
 *
 * Metadata is set page-level (overrides the layout default) so link
 * previews — when a WotC exec forwards the URL internally — surface
 * the production-ready positioning rather than the older "Fantasy
 * Bloomberg Terminal" framing.
 */

export const metadata: Metadata = {
  title: "Arcane Analytics",
  description:
    "Analytical instrumentation for the D&D ecosystem. Daily bylined analyst writing, Universes Beyond candidate ranking, and strategic signal intelligence for the tabletop industry.",
}

export default async function RootPage() {
  const session = await auth()

  // Authenticated users skip the marketing landing entirely.
  if (session?.user) {
    redirect("/overview")
  }

  return (
    <main className="flex min-h-[calc(100vh-3.5rem)] flex-col items-center justify-center gap-10 bg-obsidian px-6 py-12">
      <div className="w-full max-w-xl space-y-8 text-center">
        {/* ── Wordmark eyebrow ───────────────────────────────────── */}
        <p className="font-mono text-[11px] uppercase tracking-[0.28em] text-ember-bright">
          Arcane Analytics
        </p>

        {/* ── Tagline / hero ─────────────────────────────────────── */}
        <h1 className="font-display text-4xl font-semibold leading-tight text-parchment sm:text-5xl">
          Analytical instrumentation for the D&amp;D ecosystem.
        </h1>

        {/* ── Supporting paragraph ───────────────────────────────── */}
        <p className="mx-auto max-w-lg font-sans text-base leading-relaxed text-ash">
          We read Hasbro&rsquo;s 10-K and live cultural signal data in parallel.
          Eight calibrated analyst voices, daily. Universes Beyond candidate
          ranking across 142 non-TTRPG IPs. Hype-vs-play composite analysis.
        </p>

        {/* ── Sign-in CTA ────────────────────────────────────────── */}
        <div className="flex flex-col items-center gap-3 pt-2">
          <LandingCta />
          <p className="font-sans text-xs text-ash/70">
            Sign in with your work email.
          </p>
        </div>
      </div>
    </main>
  )
}
