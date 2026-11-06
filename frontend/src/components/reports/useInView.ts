import { useEffect, useRef, useState } from 'react'

/** true once the element has come within `rootMargin` of the viewport (sticky: never resets). */
export function useInView<T extends Element>(rootMargin = '400px') {
  const ref = useRef<T | null>(null)
  const [seen, setSeen] = useState(false)

  useEffect(() => {
    if (seen) return
    const el = ref.current
    if (!el) return
    if (typeof IntersectionObserver === 'undefined') {
      setSeen(true)
      return
    }
    const obs = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting)) {
          setSeen(true)
          obs.disconnect()
        }
      },
      { rootMargin },
    )
    obs.observe(el)
    return () => obs.disconnect()
  }, [seen, rootMargin])

  return { ref, seen }
}
