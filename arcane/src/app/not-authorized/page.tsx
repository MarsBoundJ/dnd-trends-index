/*
 * Arcane Analytics — Not Authorized (Step 12).
 *
 * Landing page when a signed-in user hits /admin/* and isn't on the
 * ADMIN_EMAILS allowlist. Friendly rather than a blunt 403 — most of
 * the time this will be someone who tried the URL out of curiosity.
 */

import Link from "next/link"

export const metadata = {
  title: "Not authorized · Arcane Analytics",
}

export default function NotAuthorizedPage() {
  return (
    <main className="min-h-[calc(100vh-3.5rem)] flex flex-col items-center justify-center gap-6 bg-obsidian px-6">
      <div className="max-w-md text-center space-y-4">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          403 · Warded
        </p>
        <h1 className="font-display text-3xl font-semibold text-parchment">
          This door is not for you.
        </h1>
        <p className="font-sans text-sm text-ash leading-relaxed">
          The admin area is gated by an allowlist — your signed-in account
          isn&rsquo;t on it. If you think that&rsquo;s a mistake, ping the
          site owner.
        </p>
      </div>

      <div className="flex gap-3">
        <Link
          href="/"
          className="inline-flex items-center justify-center rounded-lg border border-bronze bg-onyx px-5 py-2.5 font-sans text-sm font-medium text-parchment transition-colors hover:border-ember hover:bg-iron"
        >
          Back to home
        </Link>
        <Link
          href="/articles"
          className="inline-flex items-center justify-center rounded-lg border border-bronze bg-onyx px-5 py-2.5 font-sans text-sm font-medium text-ash transition-colors hover:border-ember hover:bg-iron hover:text-parchment"
        >
          Read the Council
        </Link>
      </div>
    </main>
  )
}
