# Prompts for the AI Cadre

Two self-contained briefs to paste into a fresh session (the Council personas, or
any external model). They carry their own grounding, because the failure mode
with a vague "what can we do with this data" prompt is a confident answer that
assumes sales figures we do not have.

**The constraint that shapes everything: we have almost no cardinal data.** Nearly
every stream is a *leaderboard position*, not a quantity. The two exceptions are
real and worth exploiting, and they are named below.

---

## Prompt A — What is this data actually good for?

> I run a TTRPG publishing-market intelligence project. I want your honest
> assessment of what questions this data can and cannot answer, and where the
> highest-value analysis sits. Push back on anything I seem to be assuming.
>
> **What I capture, and what each thing actually measures:**
>
> | Stream | What it measures | Shape |
> |---|---|---|
> | Amazon | Position on 7 lists (Best Sellers, New Releases, Most Wished For, across D&D books / RPG books / games & accessories / dice) plus price, star rating, review count | Ordinal rank, best-of across lists |
> | DMs Guild | Medal tier (Adamantine / Mithral / Platinum / Gold / Silver / Electrum / Copper) from bestseller shelves, plus price, page count, publisher, facet tags, star distribution, release date | Ordinal tier |
> | DriveThruRPG | Same as DMs Guild — same platform, different catalogue | Ordinal tier |
> | Roll20 marketplace | Top 200 by "popular" | Ordinal |
> | itch.io | Indie digital product listings | Listing presence |
> | BGG / RPGGeek | Ratings and ownership counts | **Ownership, not sales** |
> | Kickstarter | Backers, pledged USD, goal, % funded, status, end date | **Actual money** |
> | BackerKit | Same shape as Kickstarter | **Actual money** |
>
> I also capture context streams that are not commercial: Google Trends,
> Wikipedia pageviews, Reddit, Twitch, YouTube, Fandom wikis, AO3 fanfiction,
> D&D Beyond homebrew, Steam, mod.io, Nexus Mods.
>
> **Hard limits I already know about. Do not propose analysis that needs these:**
>
> 1. **No unit sales, anywhere, on any storefront.** A medal tier or a rank
>    position tells me relative standing on one board on one day. It does not
>    tell me how many copies moved, and two platforms' ranks are not comparable
>    in magnitude.
> 2. **Price × rank is not revenue.** DMs Guild has pay-what-you-want titles
>    where the listed price is what the creator asks, not what buyers paid.
> 3. **The streams barely overlap.** They cover different segments of the
>    business — mass-market print, community PDF, indie digital, crowdfunded
>    pre-release. A title appearing on two boards is the exception. Title-matching
>    across platforms returns few rows, and that is the correct result, not a bug.
> 4. **Capture dates differ per stream**, sometimes by weeks.
> 5. **BGG and RPGGeek measure ownership and rating, not sales.** A high
>    ownership count can reflect a decade-old book.
>
> **The two exceptions, which I suspect are the most valuable thing here:**
> Kickstarter and BackerKit carry *actual money* — pledged USD and backer counts.
> They are the only streams with cardinal data, and they sit at the START of a
> product's life.
>
> **My questions:**
>
> 1. What questions can this data answer *well* that someone with better-funded
>    access to sales figures could not answer, or would not bother to?
> 2. Given that crowdfunding is the only place with real numbers, is there a
>    defensible way to use a book's funding performance as an anchor for
>    interpreting its later ordinal position on storefronts? Or is that a bridge
>    too far?
> 3. What is the single highest-value analysis you would build first, and what
>    would make it wrong?
> 4. **What would you refuse to attempt with this data**, and what would you
>    want to see someone stop claiming?
>
> **One hypothesis to critique, not validate:** track named publishers
> (Goodman Games, Kobold Press, Darrington Press, MCDM, and others), and follow
> a single title along its lifecycle — Kickstarter or BackerKit campaign, then
> release, then position on DMs Guild / DriveThruRPG / Amazon, then ratings on
> BGG/RPGGeek. The appeal is that it turns a set of disconnected leaderboards
> into one narrative per product. Tell me where that breaks: what fraction of
> titles would actually be traceable end to end, what identity-matching problems
> it creates, and whether the resulting story would be sound or just plausible.

---

## Prompt B — Where does the absolute market data live?

> I track the TTRPG publishing market using scraped leaderboard positions from
> Amazon, DriveThruRPG, DMs Guild, Roll20, itch.io, BGG/RPGGeek, Kickstarter and
> BackerKit. I have relative standing over time. **I have no absolute figures** —
> no market size, no category revenue, no unit volume, nothing that tells me
> whether TTRPG book sales overall are rising, falling or flat.
>
> I want to know where that ground truth exists, how obtainable it is, and what
> it costs.
>
> **Specifically:**
>
> 1. **Roll20** appears to own DriveThruRPG and DMs Guild. Does it publish
>    anything — an annual industry report, an investor or partner deck, a
>    state-of-the-industry post, aggregate marketplace statistics? Historically
>    OneBookShelf published year-in-review numbers; does anything like that still
>    exist, and at what granularity?
> 2. **Industry trade sources.** ICv2 publishes hobby-channel market-size
>    estimates. Circana (formerly NPD) BookScan covers print retail. What else
>    covers this category, what does each actually measure (hobby channel vs mass
>    market vs direct), and where are the gaps a naive reader would miss?
> 3. **Public-company filings.** Hasbro/Wizards of the Coast report segment
>    revenue. Is D&D broken out at a useful granularity, or buried in a segment
>    that mixes it with unrelated lines? Does anyone else public report anything
>    relevant?
> 4. **Crowdfunding aggregates.** Kickstarter publishes category stats, and
>    third parties aggregate tabletop funding totals. How close does "total
>    pledged to tabletop RPG projects per quarter" come to being a usable proxy
>    for market direction, and what does it systematically miss?
> 5. **Anything I have not thought of** — trade associations, distributor
>    reports, printer or POD volume data, library acquisition data, customs or
>    ISBN registration data.
>
> For each source, tell me plainly: **free / paid / unobtainable**, what it
> genuinely measures versus what people assume it measures, and how it could
> mislead someone triangulating from it. I would rather have three sources I
> understand the limits of than a dozen I do not.
>
> I am not asking you to fetch or estimate any numbers. I am asking where the
> numbers live and whether they are worth pursuing.

---

## Notes for whoever runs these

**Do not soften the limits section in Prompt A.** It is there because the
obvious answers — market share, revenue estimates, cross-platform sales
rankings — are exactly the ones this data cannot support, and a model that has
not been told so will produce them fluently.

**Prompt B contains one unverified premise.** That Roll20 owns DriveThruRPG and
DMs Guild is reported, not confirmed here. What we have *observed* is narrower
and solid: the two storefronts run one platform — the same Angular frontend over
one JSON:API backend, where `groupId` and `siteId` decide which catalogue you
get. A bare call to `api.dmsguild.com` returns DriveThruRPG's taxonomy with HTTP
200. Shared infrastructure is a fact; shared ownership is a claim to verify.

**If the ownership premise holds**, it is worth noting we already capture the
Roll20 marketplace separately (`commercial_data.roll20_rankings`, top 200 by
"popular"). That would make three storefronts under one corporate parent, which
is either an analytic opportunity or a correlated-source hazard depending on how
it is used — and worth asking the Cadre which.
