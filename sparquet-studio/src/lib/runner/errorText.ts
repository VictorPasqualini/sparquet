/**
 * Telling whether two error texts are the same failure.
 *
 * A failure is recorded at every level it passes through: the step raises it,
 * the job that ran the step carries it, and the run that carried the job carries
 * it again. Each level is right to store it — a job run read on its own must
 * still say why it failed — but printing all of them at once turns one problem
 * into three cards of near-identical text, which is what a person reads as three
 * problems.
 *
 * So the rule at render time is: the deepest level on screen keeps the message,
 * the ones above it stay quiet. That needs a comparison, and equality is not it:
 * the outer level usually repeats the inner text with a prefix of its own
 * ("Stage ingest failed: …"). Containment either way is what "already on screen"
 * means here.
 */

export function isErrorText(value: string | null | undefined): value is string {
  return typeof value === 'string' && value.trim().length > 0
}

function flatten(value: string): string {
  return value.replace(/\s+/g, ' ').trim()
}

/** True when `a` and `b` are the same failure, one possibly wrapped in the other. */
export function sameErrorText(
  a: string | null | undefined,
  b: string | null | undefined,
): boolean {
  if (!isErrorText(a) || !isErrorText(b)) return false
  const left = flatten(a)
  const right = flatten(b)
  return left === right || left.includes(right) || right.includes(left)
}
