// Shared fetch client. Native /api prefix (Vite proxy does NOT rewrite).
// Errors follow the backend contract: { detail, code }.

export class ApiError extends Error {
  status: number
  code: string
  constructor(status: number, code: string, detail: string) {
    super(detail)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
  get isDbBusy(): boolean {
    return this.code === 'db_busy'
  }
}

export type Query = Record<string, string | number | boolean | null | undefined>

export function buildUrl(path: string, query?: Query): string {
  const url = path.startsWith('/') ? path : `/api/${path}`
  if (!query) return url
  const qs = new URLSearchParams()
  for (const [k, v] of Object.entries(query)) {
    if (v !== null && v !== undefined) qs.set(k, String(v))
  }
  const s = qs.toString()
  return s ? `${url}?${s}` : url
}

async function toError(res: Response): Promise<ApiError> {
  let detail = res.statusText || `HTTP ${res.status}`
  let code = 'error'
  try {
    const body = await res.json()
    if (body && typeof body === 'object') {
      if (typeof body.detail === 'string') detail = body.detail
      if (typeof body.code === 'string') code = body.code
    }
  } catch {
    /* non-JSON error body */
  }
  return new ApiError(res.status, code, detail)
}

export async function request<T>(
  method: string,
  path: string,
  opts: { query?: Query; body?: unknown; signal?: AbortSignal } = {},
): Promise<T> {
  const res = await fetch(buildUrl(path, opts.query), {
    method,
    headers: opts.body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
    signal: opts.signal,
  })
  if (!res.ok) throw await toError(res)
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

export const api = {
  get: <T>(path: string, query?: Query, signal?: AbortSignal) =>
    request<T>('GET', path, { query, signal }),
  post: <T>(path: string, body?: unknown) => request<T>('POST', path, { body }),
  put: <T>(path: string, body?: unknown) => request<T>('PUT', path, { body }),
  del: <T>(path: string) => request<T>('DELETE', path),
}

export default api
