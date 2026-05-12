/*
 * Trusight — Admin Frames panel (Step 9.5).
 *
 * Lists every frame in Firestore and shows which is currently active. Admin
 * can flip the active pointer via a server action. Frames themselves are
 * NOT editable from this UI — edit via the Firestore console or a setup
 * script (keeps accidental worldview rewrites out of a one-click flow).
 *
 * Gated by proxy.ts (`/admin/:path*` matcher + ADMIN_EMAILS allowlist).
 */

import { revalidatePath } from "next/cache"

import { AdminShell } from "@/components/admin/admin-shell"
import { requireAdmin } from "@/lib/admin-guard"
import {
  getActiveFrameId,
  listFrames,
  setActiveFrameId,
  type Frame,
} from "@/lib/frames"
import { cn } from "@/lib/utils"

export const metadata = {
  title: "Frames · Admin",
}

// ─── Server action: flip the active frame pointer ──────────────────────────
async function activateFrameAction(formData: FormData): Promise<void> {
  "use server"

  const admin = await requireAdmin()
  if (!admin) {
    throw new Error("forbidden")
  }

  const frameId = formData.get("frame_id")
  if (typeof frameId !== "string" || !frameId) {
    throw new Error("frame_id required")
  }

  await setActiveFrameId(frameId)
  revalidatePath("/admin/frames")
}

// ─── Page ──────────────────────────────────────────────────────────────────

export default async function AdminFramesPage() {
  const [frames, activeId] = await Promise.all([
    listFrames(),
    getActiveFrameId(),
  ])

  return (
    <AdminShell
      title="Frames"
      description="Corporate-strategy frames that drive Track D article generation and Sage's worldview context. Frames live as Firestore docs (frames/{id}) — edit the corpus via setup scripts, flip the active pointer from here."
      crumbs={[{ label: "Frames" }]}
    >
      {frames.length === 0 ? (
        <EmptyState />
      ) : (
        <>
          <ActivePointerPanel activeId={activeId} framesCount={frames.length} />

          <section aria-label="Frames">
            <h2 className="font-mono uppercase tracking-widest text-[10px] text-ash/80 mb-3">
              All frames
            </h2>
            <ul className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {frames.map((frame) => (
                <FrameTile
                  key={frame.frame_id}
                  frame={frame}
                  isActive={frame.frame_id === activeId}
                />
              ))}
            </ul>
          </section>
        </>
      )}
    </AdminShell>
  )
}

// ─── Subcomponents ─────────────────────────────────────────────────────────

function EmptyState() {
  return (
    <div className="border border-bronze rounded-xl bg-onyx p-8 text-center space-y-3">
      <p className="font-display text-lg font-medium text-parchment">
        No frames seeded yet.
      </p>
      <p className="font-sans text-sm text-ash max-w-md mx-auto">
        Run <code className="font-mono text-ember-bright">python setup_frames_collection.py</code>{" "}
        to seed the <code className="font-mono text-ember-bright">pure-data</code> baseline frame, then refresh.
      </p>
    </div>
  )
}

function ActivePointerPanel({
  activeId,
  framesCount,
}: {
  activeId: string | null
  framesCount: number
}) {
  return (
    <section
      aria-label="Active frame pointer"
      className="border border-bronze rounded-xl bg-onyx p-4 mb-6"
    >
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
        <div>
          <p className="font-mono uppercase tracking-widest text-[10px] text-ash/80 mb-1">
            Active frame
          </p>
          {activeId ? (
            <p className="font-display text-base text-parchment">
              <span className="text-ember-bright">{activeId}</span>
              <span className="text-ash/60 font-sans text-sm ml-2">
                (of {framesCount} total)
              </span>
            </p>
          ) : (
            <p className="font-display text-base text-ash">
              None set.{" "}
              <span className="font-sans text-sm text-ash/70">
                Flip one below to activate.
              </span>
            </p>
          )}
        </div>
      </div>
    </section>
  )
}

function FrameTile({ frame, isActive }: { frame: Frame; isActive: boolean }) {
  const buildingBlockCount = frame.strategic_building_blocks.length
  const toneRatio = `${frame.tone_distribution.deck_ready}:${frame.tone_distribution.sharp}`
  const hasWorldview = frame.worldview_summary.length > 0

  return (
    <li
      className={cn(
        "rounded-xl border p-4 space-y-3",
        isActive
          ? "border-ember bg-iron/60"
          : "border-bronze bg-onyx",
      )}
    >
      <header className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-widest text-ember/80">
            {frame.frame_id}
          </p>
          <h3 className="font-display text-sm font-semibold text-parchment mt-0.5">
            {frame.label}
          </h3>
        </div>
        {isActive && (
          <span className="font-mono text-[10px] uppercase tracking-widest text-ember-bright shrink-0 border border-ember/60 rounded px-1.5 py-0.5">
            active
          </span>
        )}
      </header>

      <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
        <dt className="text-ash/70">Worldview</dt>
        <dd className="text-parchment/90 text-right font-mono">
          {hasWorldview ? `${frame.worldview_summary.length} chars` : "—"}
        </dd>
        <dt className="text-ash/70">Strategic blocks</dt>
        <dd className="text-parchment/90 text-right font-mono">
          {buildingBlockCount}
        </dd>
        <dt className="text-ash/70">Tone (deck:sharp)</dt>
        <dd className="text-parchment/90 text-right font-mono">{toneRatio}</dd>
      </dl>

      {!isActive && (
        <form action={activateFrameAction}>
          <input type="hidden" name="frame_id" value={frame.frame_id} />
          <button
            type="submit"
            className={cn(
              "w-full inline-flex items-center justify-center rounded-md px-3 py-1.5",
              "border border-bronze/60 bg-onyx text-xs font-sans text-parchment",
              "transition-colors hover:border-ember hover:bg-iron",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian",
            )}
          >
            Activate
          </button>
        </form>
      )}
    </li>
  )
}
