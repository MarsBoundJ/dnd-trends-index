/*
 * Trusight — BackerKit Console page (Step 12).
 */

import { AdminShell } from "@/components/admin/admin-shell"
import { HarvestConsole } from "@/components/admin/harvest-console"

export const metadata = {
  title: "BackerKit Console · Admin",
}

export default function BackerkitConsolePage() {
  return (
    <AdminShell
      title="BackerKit Console"
      description="Fire the BackerKit trending-RPG harvester on demand. Each run scrapes the RPG collection, classifies projects into 5e Compatible / OSR / Other, and writes to commercial_data.backerkit_projects. The weekly scheduler still runs too — this is for catching spikes between runs."
      crumbs={[{ label: "BackerKit Console" }]}
    >
      <HarvestConsole />
    </AdminShell>
  )
}
