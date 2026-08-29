/**
 * Tags on library records — the rules, in one place.
 *
 * A tag is a free label the author puts on a Job, a Pipeline or a Workflow, and
 * what makes it worth having is that spending is aggregated by it: the runner
 * copies the tags that applied onto the ledger entry when it charges the run, so
 * "what did marketing cost us in March" is a question with an answer.
 *
 * That is also why the rules here are not cosmetic. They mirror `normalize_tags`
 * on the server exactly (`server/history.py`, `server/credits.py`), and the two
 * have to agree: a Studio that let `Prod` and `prod` coexist would produce a bill
 * split in half for a reason nobody could guess from the screen.
 */

/** As many tags as one record may carry. */
export const MAX_TAGS = 20

/** As long as one tag may be. */
export const MAX_TAG_LENGTH = 40

/**
 * The tags as they will be stored: trimmed, deduplicated, bounded.
 *
 * Deduplication is case-insensitive while the stored form keeps the case the
 * author typed — one tag, still labelled the way they wrote it.
 */
export function normalizeTags(values: readonly unknown[] | undefined | null): string[] {
  if (!Array.isArray(values)) return []
  const out: string[] = []
  const seen = new Set<string>()
  for (const value of values) {
    if (typeof value !== 'string') continue
    const tag = value.trim().slice(0, MAX_TAG_LENGTH).trim()
    if (!tag) continue
    const key = tag.toLocaleLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    out.push(tag)
    if (out.length >= MAX_TAGS) break
  }
  return out
}

/**
 * The list with `value` added, or the list unchanged.
 *
 * Unchanged covers every way an addition is a no-op — blank, already there in
 * any case, or past the ceiling — so a caller can compare by identity to know
 * whether anything happened.
 */
export function addTag(tags: readonly string[], value: string): string[] {
  const next = normalizeTags([...tags, value])
  return next.length === tags.length ? (tags as string[]) : next
}

/** The list without `value`, matched the way tags are compared: case-insensitively. */
export function removeTag(tags: readonly string[], value: string): string[] {
  const key = value.trim().toLocaleLowerCase()
  return tags.filter((tag) => tag.toLocaleLowerCase() !== key)
}

/** Whether the list already carries this tag, under any casing. */
export function hasTag(tags: readonly string[], value: string): boolean {
  const key = value.trim().toLocaleLowerCase()
  return tags.some((tag) => tag.toLocaleLowerCase() === key)
}

/**
 * Every tag in use across the library, most used first, then alphabetically.
 *
 * Feeds the suggestion list, which is what keeps a vocabulary from drifting into
 * `finance`, `financeiro` and `fin` — three rows on a bill that were meant to be
 * one.
 */
export function collectTags(records: readonly { tags?: readonly string[] }[]): string[] {
  const counts = new Map<string, { label: string; count: number }>()
  for (const record of records) {
    for (const tag of normalizeTags(record.tags ?? [])) {
      const key = tag.toLocaleLowerCase()
      const found = counts.get(key)
      if (found) found.count += 1
      else counts.set(key, { label: tag, count: 1 })
    }
  }
  return [...counts.values()]
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
    .map((entry) => entry.label)
}
