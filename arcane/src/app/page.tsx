import type { Metadata } from "next"
import Image from "next/image"
import Link from "next/link"

import { auth } from "../../auth"
import {
  SigilStackedCards,
  SigilDecisionMatrix,
  SigilSentimentWaveform,
  SigilThreeSilhouettes,
  SigilEvidenceLadder,
  SigilPageWithStar,
  SigilHexHub,
} from "@/components/sigils"

/*
 * Trusight — root landing page (cold-outreach version).
 *
 * Visitor profile assumed: a WotC / Hasbro executive who just heard a
 * voicemail or got a one-liner pointing them at the site. Goal in 10
 * seconds: surface enough credibility that they read at least one
 * report. The page therefore inverts the prior "sign-in CTA" design —
 * proof-first, login-later.
 *
 * Authenticated users skip the marketing landing and go straight to
 * /overview. The marketing copy is for first-time anonymous visitors
 * only.
 *
 * Copy is Phil's verbatim hand-write. The seven research deliverables
 * surfaced as cards correspond to the files staged in public/reports/.
 */

export const metadata: Metadata = {
  title: "Trusight: a D&D Trendwatch",
  description:
    "Data-driven intelligence for the D&D ecosystem. IP & licensing analysis, sentiment audits, and weekly community-signal reads — built on a corpus of everything that's been said about D&D online.",
}

type Report = {
  title: string
  blurb: string
  href: string
  external: boolean
  tag: string
  Sigil: React.ComponentType<{ className?: string }>
}

const REPORTS: Report[] = [
  {
    title: "IP Deep Dive: 19 Licensing Candidates",
    blurb:
      "Per-IP breakdowns of 19 licensing candidates — translation strategy, brand-integrity cost, and measured community signal. Includes Hollow Knight, Elden Ring, Berserk, Solo Leveling, Bloodborne, One Piece, FFXIV, House of the Dragon, and 11 more.",
    href: "/reports/ip_deep_dive.html",
    external: true,
    tag: "DEEP DIVE",
    Sigil: SigilStackedCards,
  },
  {
    title: "IP & Licensing Report",
    blurb:
      "Six case studies. Four risks. One framework. The decision-engine view on which IPs to license, which to soft-pass, and why.",
    href: "/reports/trusight_report_ip_licensing.pdf",
    external: true,
    tag: "REPORT",
    Sigil: SigilDecisionMatrix,
  },
  {
    title: "UA Sentiment Audit (A1 LIVE)",
    blurb:
      "Live methodology proof on a contested IP — six channels of community signal, classifier-tagged sentiment, and forum-anonymized quotes.",
    href: "/reports/trusight_report_A1_LIVE_audit.pdf",
    external: true,
    tag: "AUDIT",
    Sigil: SigilSentimentWaveform,
  },
  {
    title: "Persona Playbooks",
    blurb:
      "Three roles inside WotC — Designers, Marketers, IP & Licensing — each face a different weekly decision. Three Trusight reads for three weekly questions.",
    href: "/reports/trusight_report_part2_personas.pdf",
    external: true,
    tag: "REPORT",
    Sigil: SigilThreeSilhouettes,
  },
  {
    title: "Persona A: Worked Proofs",
    blurb:
      "Evidence stack for the Designer persona. Worked examples on what data Trusight surfaces and how it informs design-side decisions.",
    href: "/reports/trusight_report_part2_proofs_personaA.pdf",
    external: true,
    tag: "REPORT",
    Sigil: SigilEvidenceLadder,
  },
  {
    title: "IP One-pager",
    blurb:
      "The 12-candidate IP decision engine at a glance. Four-quadrant fit/reception map across an 8-source signal stack.",
    href: "/reports/trusight_onepager_ip.pdf",
    external: true,
    tag: "ONE-PAGER",
    Sigil: SigilPageWithStar,
  },
  {
    title: "Capabilities One-pager",
    blurb:
      "Beyond IP & Licensing — what else Trusight does. Designers, marketers, trial-balloon early-warning, Playing-to-Win mapping.",
    href: "/reports/trusight_onepager_capabilities.pdf",
    external: true,
    tag: "ONE-PAGER",
    Sigil: SigilHexHub,
  },
]

export default async function RootPage() {
  const session = await auth()

  // No auto-redirect for signed-in users. Auto-bouncing them to
  // /overview breaks the Atlas's Home tile (clicking it would
  // instantly redirect them back to /overview, making the tile a
  // no-op). Signed-in users see the same marketing page with the
  // bottom CTA swapped from "Sign in" to a Trends jump-link.

  return (
    <main className="bg-obsidian">
      {/* ── Hero ───────────────────────────────────────────────────── */}
      <section className="mx-auto flex max-w-4xl flex-col items-center px-6 pb-16 pt-16 sm:pt-24">
        <Image
          src="/logos/trusight_logo_4k_dark.png"
          alt="Trusight"
          width={520}
          height={84}
          priority
          className="h-auto w-full max-w-md"
        />
      </section>

      {/* ── Pitch copy (Phil's verbatim) ───────────────────────────── */}
      <section className="mx-auto max-w-3xl px-6 pb-20">
        <div className="space-y-6 font-sans text-lg leading-relaxed text-parchment sm:text-xl">
          <p>
            What if we gathered EVERYTHING about D&amp;D on the web? Then took
            that EVERYTHING and stored it in a database? Then, what if we asked
            the most sophisticated AI to analyze, parse, and understand that
            data?
          </p>
          <p className="font-display text-2xl font-semibold text-ember-bright sm:text-3xl">
            That&rsquo;s Trusight.
          </p>
          <p>
            With Trusight, if it&rsquo;s about D&amp;D, you can dive deep. Want
            to learn more about IP &amp; Licensing prospects? Check. Want to
            know how a UA landed? Check. Want to know what the community is
            searching? Check!
          </p>
          <p>
            Take a look at our reports, then imagine the possibilities.{" "}
            <span className="font-display font-semibold text-ember-bright">
              Trusight, THE trendwatch for D&amp;D.
            </span>
          </p>
        </div>
      </section>

      {/* ── Reports section ────────────────────────────────────────── */}
      <section className="mx-auto max-w-6xl px-6 pb-24">
        <div className="mb-10 flex items-end justify-between">
          <h2 className="font-display text-2xl font-semibold text-parchment sm:text-3xl">
            Recent reports
          </h2>
          <p className="font-sans text-sm text-ash hidden sm:block">
            Click any report to read in your browser.
          </p>
        </div>

        <div className="grid grid-cols-1 gap-5 md:grid-cols-2 lg:grid-cols-3">
          {REPORTS.map((report) => (
            <a
              key={report.href}
              href={report.href}
              target="_blank"
              rel="noopener noreferrer"
              className="group flex flex-col gap-3 rounded-lg border border-bronze/40 bg-iron/40 p-5 transition hover:border-ember hover:bg-iron/70 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian"
            >
              <div className="flex items-center gap-2 text-ember-bright">
                <report.Sigil className="h-5 w-5 shrink-0" />
                <span className="font-mono text-[10px] uppercase tracking-[0.18em]">
                  {report.tag}
                </span>
              </div>
              <h3 className="font-display text-lg font-semibold leading-snug text-parchment group-hover:text-ember-bright">
                {report.title}
              </h3>
              <p className="font-sans text-sm leading-relaxed text-ash">
                {report.blurb}
              </p>
              <span
                aria-hidden
                className="mt-auto font-mono text-xs text-ember-bright/80 group-hover:text-ember-bright"
              >
                Read →
              </span>
            </a>
          ))}
        </div>
      </section>

      {/* ── Bottom CTA — sign-in for anon, jump-to-app for signed-in ── */}
      <section className="border-t border-bronze/30 bg-onyx/40 py-10">
        <div className="mx-auto flex max-w-4xl flex-col items-center gap-3 px-6 text-center">
          {session?.user ? (
            <>
              <p className="font-sans text-sm text-ash">Welcome back.</p>
              <Link
                href="/overview"
                className="font-sans text-sm font-medium text-ember-bright hover:text-ember-bright/80 hover:underline"
              >
                Continue to Trends →
              </Link>
            </>
          ) : (
            <>
              <p className="font-sans text-sm text-ash">
                Already have an account?
              </p>
              <Link
                href="/api/auth/signin"
                className="font-sans text-sm font-medium text-ember-bright hover:text-ember-bright/80 hover:underline"
              >
                Sign in →
              </Link>
            </>
          )}
        </div>
      </section>
    </main>
  )
}
