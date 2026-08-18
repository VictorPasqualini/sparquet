/**
 * Landing motion.
 *
 * Three behaviors, all opt-in through a class or a data attribute, all no-ops
 * when the visitor asked for reduced motion:
 *
 *   .reveal          fades in once, when it first reaches the viewport
 *   .spotlight       tracks the pointer so a card can light up under it
 *   [data-stream-*]  types out the JSON the assistant "generated", once
 *
 * Everything degrades to the plain page if this script never runs.
 */

const prefersReducedMotion = (): boolean =>
  window.matchMedia('(prefers-reduced-motion: reduce)').matches

function setupReveal(): void {
  const targets = document.querySelectorAll<HTMLElement>('.reveal, .rule-draw')
  if (targets.length === 0) return

  if (prefersReducedMotion() || !('IntersectionObserver' in window)) {
    targets.forEach((element) => element.classList.add('is-visible'))
    return
  }

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue
        entry.target.classList.add('is-visible')
        observer.unobserve(entry.target)
      }
    },
    // Trigger a little before the element is fully on screen, so the motion
    // finishes about when the reader's eye arrives.
    { rootMargin: '0px 0px -12% 0px', threshold: 0.1 },
  )

  targets.forEach((element) => observer.observe(element))
}

function setupSpotlight(): void {
  if (prefersReducedMotion()) return
  if (!window.matchMedia('(hover: hover)').matches) return

  const cards = document.querySelectorAll<HTMLElement>('.spotlight')
  cards.forEach((card) => {
    card.addEventListener('pointermove', (event) => {
      const bounds = card.getBoundingClientRect()
      card.style.setProperty('--pointer-x', `${event.clientX - bounds.left}px`)
      card.style.setProperty('--pointer-y', `${event.clientY - bounds.top}px`)
    })
  })
}

/** Types `data-stream-text` into the element, a few characters per frame. */
function streamInto(element: HTMLElement): void {
  const text = element.dataset.streamText ?? ''
  const root = element.closest<HTMLElement>('[data-stream-root]')
  const status = root?.querySelector<HTMLElement>('[data-stream-status]')

  if (prefersReducedMotion()) {
    element.textContent = text
    element.classList.remove('caret')
    if (status) status.textContent = 'ready'
    return
  }

  let index = 0
  const step = () => {
    // A burst per frame reads like a model streaming, not a typewriter.
    index = Math.min(text.length, index + 3)
    element.textContent = text.slice(0, index)
    if (index < text.length) {
      requestAnimationFrame(step)
      return
    }
    element.classList.remove('caret')
    if (status) status.textContent = 'ready'
  }

  requestAnimationFrame(step)
}

function setupStream(): void {
  const targets = document.querySelectorAll<HTMLElement>('[data-stream-target]')
  if (targets.length === 0) return

  if (!('IntersectionObserver' in window)) {
    targets.forEach(streamInto)
    return
  }

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue
        observer.unobserve(entry.target)
        streamInto(entry.target as HTMLElement)
      }
    },
    { threshold: 0.35 },
  )

  targets.forEach((element) => observer.observe(element))
}

function init(): void {
  setupReveal()
  setupSpotlight()
  setupStream()
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init, { once: true })
} else {
  init()
}
