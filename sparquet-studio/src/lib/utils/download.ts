/**
 * Browser side-effects for handing text to the user: saving it as a file and
 * putting it on the clipboard. Both are DOM-only and must run from an event
 * handler — browsers reject downloads and clipboard writes without a gesture.
 */

/** Saves `text` as `filename`, downloaded straight from memory. */
export function downloadText(filename: string, text: string, mime = 'application/json'): void {
  const blob = new Blob([text], { type: `${mime};charset=utf-8` })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')

  anchor.href = url
  anchor.download = filename
  anchor.rel = 'noopener'
  anchor.style.display = 'none'
  document.body.append(anchor)
  anchor.click()
  anchor.remove()

  // Revoking in the same tick cancels the download in Firefox and Safari.
  window.setTimeout(() => URL.revokeObjectURL(url), 2000)
}

/**
 * Copies `text` to the clipboard, reporting whether it landed.
 * Falls back to a hidden textarea when the async API is unavailable —
 * it needs a secure context and a granted permission, neither guaranteed.
 */
export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    return copyViaTextarea(text)
  }
}

function copyViaTextarea(text: string): boolean {
  const area = document.createElement('textarea')
  area.value = text
  area.setAttribute('readonly', '')
  area.style.position = 'fixed'
  area.style.top = '0'
  area.style.opacity = '0'
  document.body.append(area)

  const selection = document.getSelection()
  const previous = selection && selection.rangeCount > 0 ? selection.getRangeAt(0) : null

  area.select()
  let copied = false
  try {
    copied = document.execCommand('copy')
  } catch {
    copied = false
  }

  area.remove()
  if (selection && previous) {
    selection.removeAllRanges()
    selection.addRange(previous)
  }
  return copied
}
