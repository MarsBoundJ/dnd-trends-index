import type { Metadata } from "next"
import Image from "next/image"
import Link from "next/link"
import { redirect } from "next/navigation"

import { auth } from "../../auth"

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
  badge: string
}

const REPORTS: Report[] = [
  {
    title: "IP Deep Dive: 19 Licensing Candidates",
    blurb:
      "Per-IP breakdowns of 19 licensing candidates — translation strategy, brand-integrity cost, and measured community signal. Includes Hollow Knight, Elden Ring, Berserk, Solo Leveling, Bloodborne, One Piece, FFXIV, House of the Dragon, and 11 more.",
    href: "/reports/ip_deep_dive.html",
    external: true,
    badge: "Flagship report",
  },
  {
    title: "IP & Licensing Report",
    blurb:
      "Six case studies. Four risks. One framework. The decision-engine view on which IPs to license, which to soft-pass, and why.",
    href: "/reports/trusight_report_ip_licensing.pdf",
    external: true,
    badge: "Companion report",
  },
  {
    title: "UA Sentiment Audit (A1 LIVE)",
    blurb:
      "Live methodology proof on a contested IP — six channels of community signal, classifier-tagged sentiment, and forum-anonymized quotes.",
    href: "/reports/trusight_report_A1_LIVE_audit.pdf",
    external: true,
    badge: "Methodology",
  },
  {
    title: "Persona Playbooks",
    blurb:
      "Three roles inside WotC — Designers, Marketers, IP & Licensing — each face a different weekly decision. Three Trusight reads for three weekly questions.",
    href: "/reports/trusight_report_part2_personas.pdf",
    external: true,
    badge: "Personas",
  },
  {
    title: "Persona A: Worked Proofs",
    blurb:
      "Evidence stack for the Designer persona. Worked examples on what data Trusight surfaces and how it informs design-side decisions.",
    href: "/reports/trusight_report_part2_proofs_personaA.pdf",
    external: true,
    badge: "Proofs",
  },
  {
    title: "IP One-pager",
    blurb:
      "The 12-candidate IP decision engine at a glance. Four-quadrant fit/reception map across an 8-source signal stack.",
    href: "/reports/trusight_onepager_ip.pdf",
    external: true,
    badge: "One-pager",
  },
  {
    title: "Capabilities One-pager",
    blurb:
      "Beyond IP & Licensing — what else Trusight does. Designers, marketers, OGL-style early-warning, Playing-to-Win mapping.",
    href: "/reports/trusight_onepager_capabilities.pdf",
    external: true,
    badge: "One-pager",
  },
]

export default async function RootPage() {
  const session = await auth()

  // Authenticated users skip the marketing landing entirely.
  if (session?.user) {
    redirect("/overview")
  }

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
            What if you pulled EVERYTHING on the web that was about D&amp;D?
            Then took all that EVERYTHING and stored it in massive databases?
            What if you then used the most sophisticated AI to analyze, parse,
            and understand the trends from that data?
          </p>
          <p className="font-display text-2xl font-semibold text-ember-bright sm:text-3xl">
            That&rsquo;s Trusight.
          </p>
          <p>
            With Trusight, if it&rsquo;s about D&amp;D, you can dive deep. Want
            to know about IP &amp; Licensing prospects? Check. Want to know how
            a UA landed? Or, know what the D&amp;D community is hotly searching
            for this week? Check. Check. Check.
          </p>
          <p>
            Take a look at some recent reports we&rsquo;ve developed, then
            imagine the possibilities.{" "}
            <span className="font-display font-semibold text-ember-bright">
              Trusight, THE D&amp;D trendwatch.
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
              <span className="font-mono text-[10px] uppercase tracking-[0.18em] text-ember-bright">
                {report.badge}
              </span>
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

      {/* ── Subtle sign-in for return visitors ─────────────────────── */}
      <section className="border-t border-bronze/30 bg-onyx/40 py-10">
        <div className="mx-auto flex max-w-4xl flex-col items-center gap-3 px-6 text-center">
          <p className="font-sans text-sm text-ash">
            Already have an account?
          </p>
          <Link
            href="/api/auth/signin"
            className="font-sans text-sm font-medium text-ember-bright hover:text-ember-bright/80 hover:underline"
          >
            Sign in →
          </Link>
        </div>
      </section>
    </main>
  )
}
