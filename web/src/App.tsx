import { useState, type FormEvent, type ReactNode } from 'react'
import { formatJSON, request, statuses, type Attempt, type Job, type JobPage, type Worker } from './api'
import { errorMessage, useResource } from './useResource'

const labels: Record<string, string> = {
  QUEUED: 'Queued', RUNNING: 'Running', COMPLETED: 'Completed', FAILED: 'Failed',
  DEAD_LETTER: 'Dead letter', CANCELLED: 'Cancelled', ONLINE: 'Online', STALE: 'Stale',
  SCHEDULED: 'Scheduled', CANCELLATION_REQUESTED: 'Cancellation requested',
}

const dateFormat = new Intl.DateTimeFormat(undefined, {
  month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit',
  second: '2-digit', timeZoneName: 'short',
})
const date = (value?: string) => value ? dateFormat.format(new Date(value)) : '—'
const route = (id: string) => `/jobs/${encodeURIComponent(id)}`

function Status({ status }: { status: string }) {
  return <span className="status" data-status={status}><span aria-hidden="true" />{labels[status]}</span>
}

function jobStatus(job: Job) {
  if (job.status === 'RUNNING' && job.cancel_requested_at) return 'CANCELLATION_REQUESTED'
  if (job.status === 'QUEUED' && job.scheduled_at && job.attempt_count === 0 && Date.parse(job.scheduled_at) > Date.now()) return 'SCHEDULED'
  return job.status
}

function LoadState({ error, children }: { error?: string; children: ReactNode }) {
  return error ? <p className="notice error" role="alert">{error}</p> : <p className="notice" role="status">{children}</p>
}

export function App() {
  const [section, setSection] = useState<'jobs' | 'workers'>('jobs')
  const [selected, setSelected] = useState<string>()
  const [submitting, setSubmitting] = useState(false)
  const [revision, setRevision] = useState(0)
  const refresh = () => setRevision((value) => value + 1)

  return <>
    <header className="topbar">
      <div className="shell topbar-inner">
        <div className="brand"><span aria-hidden="true" className="brand-mark">↳</span>JobFlow</div>
        <nav aria-label="Operations">
          <button aria-current={section === 'jobs' ? 'page' : undefined} onClick={() => setSection('jobs')}>Jobs</button>
          <button aria-current={section === 'workers' ? 'page' : undefined} onClick={() => setSection('workers')}>Workers</button>
        </nav>
      </div>
    </header>
    <main className="shell">
      <div className="page-heading">
        <h1>{section === 'jobs' ? 'Jobs' : 'Workers'}</h1>
        <div className="actions">
          <button className="button" onClick={refresh}>Refresh</button>
          {section === 'jobs' && <button className="button primary" aria-expanded={submitting} onClick={() => setSubmitting(!submitting)}>
            {submitting ? 'Close form' : 'Submit job'}
          </button>}
        </div>
      </div>
      <div hidden={section !== 'jobs'}>
        {section === 'jobs' && submitting && <SubmitJob onSubmit={(id) => {
          setSubmitting(false)
          setSelected(id)
          refresh()
        }} />}
        <JobList revision={revision} selected={selected} onSelect={setSelected} />
        {section === 'jobs' && selected && <JobDetail key={selected} id={selected} revision={revision} onChange={refresh} onClose={() => setSelected(undefined)} />}
      </div>
      {section === 'workers' && <Workers revision={revision} />}
    </main>
  </>
}

function JobList({ revision, selected, onSelect }: { revision: number; selected?: string; onSelect: (id: string) => void }) {
  const [filters, setFilters] = useState('')
  const [pages, setPages] = useState<string[]>([''])
  const query = new URLSearchParams(filters)
  query.set('limit', '25')
  const cursor = pages[pages.length - 1]
  if (cursor) query.set('cursor', cursor)
  const page = useResource<JobPage>(`/jobs?${query}`, revision)
  const next = page?.value?.next_cursor

  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    const params = new URLSearchParams()
    for (const name of ['q', 'type', 'status']) {
      const value = String(data.get(name)).trim()
      if (value) params.set(name, value)
    }
    setFilters(params.toString())
    setPages([''])
  }

  return <section className="panel" aria-label="Job list">
    <form className="filters" onSubmit={apply}>
      <label>Search<input name="q" maxLength={128} placeholder="Job ID or task prefix" /></label>
      <label>Task type<input name="type" maxLength={128} placeholder="All task types" /></label>
      <div className="grid gap-1.5">
        <label htmlFor="job-status">Status</label>
        <select id="job-status" name="status"><option value="">All statuses</option>
          {statuses.map((status) => <option key={status} value={status}>{labels[status]}</option>)}
        </select>
      </div>
      <button className="button" type="submit">Apply filters</button>
    </form>
    {!page?.value ? <LoadState error={page?.error}>Loading jobs…</LoadState> : page.value.jobs.length === 0
      ? <LoadState>No jobs match these filters.</LoadState> : <div className="table-scroll">
        <table>
          <thead><tr><th scope="col">Job ID</th><th scope="col">Task</th><th scope="col">Status</th><th scope="col">Attempts</th><th scope="col">Created</th></tr></thead>
          <tbody>{page.value.jobs.map((job) => <tr key={job.id} data-selected={job.id === selected}>
            <td><button className="job-link" aria-pressed={job.id === selected} onClick={() => onSelect(job.id)}>{job.id}</button></td>
            <td>{job.type}</td><td><Status status={jobStatus(job)} /></td><td>{job.attempt_count}</td><td>{date(job.created_at)}</td>
          </tr>)}</tbody>
        </table>
      </div>}
    <div className="pagination">
      <span>Page {pages.length}</span>
      <button className="button" disabled={pages.length === 1} onClick={() => setPages((value) => value.slice(0, -1))}>Previous</button>
      <button className="button" disabled={!next} onClick={() => { if (next) setPages((value) => [...value, next]) }}>Next</button>
    </div>
  </section>
}

function SubmitJob({ onSubmit }: { onSubmit: (id: string) => void }) {
  const [pending, setPending] = useState(false)
  const [error, setError] = useState<string>()
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(undefined)
    setPending(true)
    try {
      const data = new FormData(event.currentTarget)
      const payload = String(data.get('payload'))
      // Validate structure locally while forwarding the original digits and keys to the server.
      const parsed: unknown = JSON.parse(payload)
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) throw new Error('Payload must be a JSON object.')
      const key = String(data.get('key'))
      if (key && !/^[!-~]{1,200}$/.test(key)) {
        throw new Error('Idempotency key must contain 1–200 visible ASCII characters without whitespace.')
      }
      const schedule = String(data.get('schedule'))
      const options = JSON.stringify({
        type: 'add_numbers',
        max_retries: Number(data.get('retries')),
        timeout_seconds: Number(data.get('timeout')),
        scheduled_at: schedule || undefined,
      })
      const { job_id } = await request<{ job_id: string }>('/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(key && { 'Idempotency-Key': key }) },
        body: `${options.slice(0, -1)},"payload":${payload}}`,
      })
      onSubmit(job_id)
    } catch (error) {
      setError(errorMessage(error))
    } finally {
      setPending(false)
    }
  }
  return <form className="panel submit" onSubmit={submit}>
    <h2>Submit add_numbers</h2>
    <label>Payload<textarea className="font-mono" name="payload" defaultValue={'{"a": 20, "b": 22}'} rows={3} required /></label>
    <div className="form-grid">
      <label>Max retries<input name="retries" type="number" min={0} max={100} defaultValue={0} required /></label>
      <label>Timeout (seconds)<input name="timeout" type="number" min={1} max={86400} defaultValue={300} required /></label>
      <label>Schedule (RFC3339)<input name="schedule" placeholder="2027-01-15T09:00:00Z" /></label>
      <label>Idempotency key<input name="key" maxLength={200} placeholder="Optional" /></label>
    </div>
    {error && <p className="error" role="alert">{error}</p>}
    <button className="button primary" type="submit" disabled={pending}>{pending ? 'Submitting…' : 'Submit'}</button>
  </form>
}

function JobDetail({ id, revision, onChange, onClose }: { id: string; revision: number; onChange: () => void; onClose: () => void }) {
  const detail = useResource<Job>(route(id), revision)
  const history = useResource<{ attempts: Attempt[] }>(`${route(id)}/attempts`, revision)
  const [pending, setPending] = useState(false)
  const [error, setError] = useState<string>()
  const [feedback, setFeedback] = useState<string>()
  const job = detail?.value
  async function action(operation: 'retry' | 'cancel') {
    setPending(true)
    setError(undefined)
    setFeedback(undefined)
    try {
      const response = await request<{ status: string }>(`${route(id)}/${operation}`, { method: 'POST' })
      setFeedback(operation === 'retry' ? 'Retry queued.' : response.status === 'RUNNING' ? 'Cancellation requested.' : 'Job cancelled.')
    } catch (error) {
      setError(errorMessage(error))
    } finally {
      onChange()
      setPending(false)
    }
  }
  return <section className="panel detail" aria-label="Job details">
    <div className="detail-heading"><h2>Job details</h2><button className="button" onClick={onClose}>Close details</button></div>
    <p className="identifier">{id}</p>
    {!job ? <LoadState error={detail?.error}>Loading job…</LoadState> : <>
      <div className="detail-heading">
        <div className="actions"><strong>{job.type}</strong><Status status={jobStatus(job)} /></div>
        <div className="actions">
          {(job.status === 'FAILED' || job.status === 'DEAD_LETTER') && <button className="button" disabled={pending} onClick={() => void action('retry')}>Retry job</button>}
          {((job.status === 'QUEUED' || job.status === 'RUNNING') && !job.cancel_requested_at) &&
            <button className="button danger" disabled={pending} onClick={() => void action('cancel')}>Cancel job</button>}
        </div>
      </div>
      {feedback && <p role="status">{feedback}</p>}
      {error && <p className="error" role="alert">{error}</p>}
      <dl className="metadata">
        <div><dt>Created</dt><dd>{date(job.created_at)}</dd></div>
        <div><dt>Scheduled for</dt><dd>{date(job.scheduled_at)}</dd></div>
        {job.status === 'QUEUED' && <div><dt>Eligible at</dt><dd>{date(job.next_run_at)}</dd></div>}
        <div><dt>Started</dt><dd>{date(job.started_at)}</dd></div>
        <div><dt>Finished</dt><dd>{date(job.completed_at)}</dd></div>
        {job.cancel_requested_at && <div><dt>Cancellation requested at</dt><dd>{date(job.cancel_requested_at)}</dd></div>}
        <div><dt>Attempts / Max retries per cycle</dt><dd>{job.attempt_count} / {job.max_retries}</dd></div>
        <div><dt>Timeout</dt><dd>{job.timeout_seconds}s</dd></div>
        {job.lease_owner && <div><dt>Lease owner</dt><dd className="identifier">{job.lease_owner}</dd></div>}
      </dl>
      <div className="form-grid">
        <div><h3>Payload</h3><pre>{formatJSON(job.payload)}</pre></div>
        {job.result !== undefined && <div><h3>Result</h3><pre>{formatJSON(job.result)}</pre></div>}
        {job.last_error && <div><h3>Last error</h3><pre className="error">{job.last_error}</pre></div>}
      </div>
    </>}
    <h3>Attempt history</h3>
    {!history?.value ? <LoadState error={history?.error}>Loading attempts…</LoadState>
      : history.value.attempts.length === 0 ? <LoadState>No attempts yet.</LoadState>
      : <div className="table-scroll"><table>
        <thead><tr><th scope="col">Attempt</th><th scope="col">Status</th><th scope="col">Worker / token</th><th scope="col">Timing</th><th scope="col">Outcome</th></tr></thead>
        <tbody>{history.value.attempts.map((attempt) => <tr key={attempt.attempt_number}>
          <td>{attempt.attempt_number}</td><td><Status status={attempt.status} /></td>
          <td className="identifier">{attempt.worker_id}<br />Token {attempt.fencing_token.toString()}</td>
          <td>{date(attempt.started_at)}<br />{date(attempt.completed_at)}</td>
          <td>{attempt.last_error ? <span className="error">{attempt.last_error}</span>
            : attempt.result !== undefined ? <pre>{formatJSON(attempt.result)}</pre> : '—'}</td>
        </tr>)}</tbody>
      </table></div>}
  </section>
}

function Workers({ revision }: { revision: number }) {
  const response = useResource<{ workers: Worker[] }>('/workers', revision)
  return <section className="panel" aria-label="Worker status">
    <p className="panel-caption">Online workers reported a heartbeat within the last 30 seconds.</p>
    {!response?.value ? <LoadState error={response?.error}>Loading workers…</LoadState>
      : response.value.workers.length === 0 ? <LoadState>No workers have registered.</LoadState>
      : <div className="table-scroll"><table>
        <thead><tr><th scope="col">Worker ID</th><th scope="col">Status</th><th scope="col">Last heartbeat</th><th scope="col">Started</th></tr></thead>
        <tbody>{response.value.workers.map((worker) => <tr key={worker.id}>
          <td className="identifier">{worker.id}</td><td><Status status={worker.status} /></td>
          <td>{date(worker.heartbeat_at)}</td><td>{date(worker.started_at)}</td>
        </tr>)}</tbody>
      </table></div>}
  </section>
}
