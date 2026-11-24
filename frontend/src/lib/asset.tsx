import { createContext, useContext } from 'react'

const KEY = 'cq_last_asset'

export const AssetContext = createContext<string>('solusdt')

/** The asset of the current `/a/:asset/...` route. */
export const useAsset = () => useContext(AssetContext)

export function loadLastAsset(): string | null {
  try {
    return localStorage.getItem(KEY)
  } catch {
    return null
  }
}

export function saveLastAsset(id: string) {
  try {
    localStorage.setItem(KEY, id)
  } catch {
    /* storage unavailable */
  }
}
