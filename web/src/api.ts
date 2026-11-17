import { parse, parseLosslessNumber, stringify } from 'lossless-json'

export const statuses = ['QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'DEAD_LETTER', 'CANCELLED'] as const
export type JobStatus = typeof statuses[number]

export interface Job {
  id: string
  type: string
  status: JobStatus
  payload: unknown
  result?: unknown
  last_error?: string
  created_at: string
  scheduled_at?: string
  next_run_at: string
  started_at?: string
  completed_at?: string
  cancel_requested_at?: string
  lease_owner?: string
  attempt_count: number
  max_retries: number
  timeout_seconds: number
}

export interface Attempt {
  attempt_number: number
  fencing_token: { toString(): string }
  worker_id: string
  status: 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED'
  started_at: string
  completed_at?: string
  result?: unknown
  last_error?: string
}

export interface Worker {
  id: string
  status: 'ONLINE' | 'STALE'
  started_at: string
  heartbeat_at: string
}

export interface JobPage {
  jobs: Job[]
  next_cursor?: string
}

export async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`/api/v1${path}`, {
    ...options,
    signal: options?.signal
      ? AbortSignal.any([options.signal, AbortSignal.timeout(10_000)])
      : AbortSignal.timeout(10_000),
  })
  const text = await response.text()
  let body: unknown
  try {
    body = parse(text, null, {
      parseNumber: (value) => Number.isSafeInteger(Number(value)) && /^-?\d+$/.test(value)
        ? Number(value)
        : parseLosslessNumber(value),
    })
  } catch {
    if (response.ok) throw new Error('JobFlow returned an invalid response.')
  }
  if (!response.ok) {
    const error = body && typeof body === 'object' && 'error' in body ? body.error : undefined
    throw new Error(typeof error === 'string' ? error : `Request failed (HTTP ${response.status}).`)
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('JobFlow returned an invalid response.')
  return body as T
}

export const formatJSON = (value: unknown) => stringify(value, undefined, 2)
