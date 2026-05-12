/*
 * Trusight — Harvesting Cockpit (Step 12).
 *
 * Server Component — pulls the bookmarklet registry (which reads from
 * scripts/*.txt on disk via `"server-only"`) and hands the raw
 * `javascript:...` payloads to the client card for drag-install.
 */

import { AdminShell } from "@/components/admin/admin-shell"
import { BookmarkletCard } from "@/components/admin/bookmarklet-card"
import { getBookmarklets } from "@/lib/bookmarklets"

export const metadata = {
  title: "Harvesting Cockpit · Admin",
}

export default function HarvestingCockpitPage() {
  const bookmarklets = getBookmarklets()

  return (
    <AdminShell
      title="Harvesting Cockpit"
      description="Bookmarklet-based manual harvests. Each pill below drags into your browser's bookmarks bar as an executable bookmark — click it while on the target site and the harvester runs against the page's DOM inside your real session."
      crumbs={[{ label: "Harvesting Cockpit" }]}
    >
      <section
        aria-label="Bookmarklet launchers"
        className="grid grid-cols-1 md:grid-cols-2 gap-4"
      >
        {bookmarklets.map((bm) => (
          <BookmarkletCard
            key={bm.id}
            id={bm.id}
            name={bm.name}
            targetHost={bm.targetHost}
            description={bm.description}
            usage={bm.usage}
            href={bm.href}
          />
        ))}
      </section>

      <section className="border-t border-bronze/40 pt-6">
        <h2 className="font-mono uppercase tracking-widest text-[10px] text-ash/80 mb-2">
          Why bookmarklets
        </h2>
        <p className="text-sm text-ash leading-relaxed max-w-3xl">
          Running these scripts from your real browser session sidesteps every
          bot-detection layer on Amazon, Kickstarter, and friends. The server
          never has to solve a CAPTCHA or maintain a stable session —
          it just receives the JSON the bookmarklet POSTs to Bouncer.
        </p>
      </section>
    </AdminShell>
  )
}
