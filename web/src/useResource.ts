import { useEffect, useState } from 'react'
import { request } from './api'

type Read<T> = { value: T; error?: never } | { error: string; value?: never }

export function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Request failed.'
}

export function useResource<T>(path: string, revision: number) {
  const [response, setResponse] = useState<{ path: string; read: Read<T> }>()
  useEffect(() => {
    const controller = new AbortController()
    let pending = false
    async function load() {
      if (pending) return
      pending = true
      try {
        const value = await request<T>(path, { signal: controller.signal })
        if (!controller.signal.aborted) setResponse({ path, read: { value } })
      } catch (error) {
        if (!controller.signal.aborted) setResponse({ path, read: { error: errorMessage(error) } })
      } finally {
        pending = false
      }
    }
    void load()
    const timer = window.setInterval(load, 5000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [path, revision])
  return response?.path === path ? response.read : undefined
}
