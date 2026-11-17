import { expect, test } from '@playwright/test'

const now = new Date().toISOString()
const scheduled = new Date(Date.now() + 86_400_000).toISOString()
const job = {
  id: 'job_demo', type: 'add_numbers', payload: { a: 20, b: 22 }, status: 'QUEUED',
  created_at: now, scheduled_at: scheduled, next_run_at: scheduled,
  attempt_count: 0, max_retries: 0, timeout_seconds: 300,
}

test('shows loading, empty and error states and can refresh after an error', async ({ page }) => {
  let release!: () => void
  const delay = new Promise<void>((resolve) => { release = resolve })
  await page.route('**/api/v1/jobs?*', async (route) => {
    const search = new URL(route.request().url()).searchParams.get('q')
    if (search) {
      await route.fulfill({ status: 503, json: { error: 'could not list jobs' } })
    } else {
      await delay
      await route.fulfill({ json: { jobs: [] } })
    }
  })
  await page.goto('/')
  await expect(page.getByText('Loading jobs…')).toBeVisible()
  release()
  await expect(page.getByText('No jobs match these filters.')).toBeVisible()
  await page.getByLabel('Search').fill('unavailable')
  await page.getByRole('button', { name: 'Apply filters' }).click()
  await expect(page.getByRole('alert')).toHaveText('could not list jobs')
  await page.unroute('**/api/v1/jobs?*')
  await page.route('**/api/v1/jobs?*', (route) => route.fulfill({ json: { jobs: [job] } }))
  await page.getByRole('button', { name: 'Refresh' }).click()
  await expect(page.getByText('Scheduled', { exact: true })).toBeVisible()
})

test('uses a cursor and requests cancellation of an active attempt', async ({ page }) => {
  const running = { ...job, id: 'job_running', status: 'RUNNING', attempt_count: 1, scheduled_at: undefined }
  let pending = false
  await page.route('**/api/v1/jobs?*', (route) => {
    const cursor = new URL(route.request().url()).searchParams.get('cursor')
    return route.fulfill({ json: cursor
      ? { jobs: [{ ...running, cancel_requested_at: pending ? now : undefined }] }
      : { jobs: [job], next_cursor: 'older' } })
  })
  await page.route('**/api/v1/jobs/job_running', (route) =>
    route.fulfill({ json: { ...running, cancel_requested_at: pending ? now : undefined } }))
  await page.route('**/api/v1/jobs/job_running/attempts', (route) => route.fulfill({ json: {
    attempts: [{ status: 'RUNNING', attempt_number: 1, fencing_token: 1, worker_id: 'worker-1', started_at: now }],
  } }))
  await page.route('**/api/v1/jobs/job_running/cancel', (route) => {
    expect(route.request().method()).toBe('POST')
    pending = true
    return route.fulfill({ status: 202, json: { ...running, cancel_requested_at: now } })
  })
  await page.goto('/')
  await expect(page.getByText('Scheduled', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Next' }).click()
  await expect(page.getByText('Page 2')).toBeVisible()
  await page.getByRole('button', { name: 'job_running' }).click()
  const detail = page.getByRole('region', { name: 'Job details' })
  await expect(detail.getByText('worker-1')).toBeVisible()
  await detail.getByRole('button', { name: 'Cancel job' }).click()
  await expect(detail.getByText('Cancellation requested', { exact: true })).toBeVisible()
  await expect(detail.getByRole('button', { name: 'Cancel job' })).toHaveCount(0)
  await page.getByRole('button', { name: 'Previous' }).click()
  await expect(page.getByText('Page 1')).toBeVisible()
})

test('rejects a whitespace-padded key and submits the operator’s explicit schedule unchanged', async ({ page }) => {
  await page.route('**/api/v1/jobs?*', (route) => route.fulfill({ json: { jobs: [] } }))
  await page.route('**/api/v1/jobs/job_demo', (route) => route.fulfill({ json: job }))
  await page.route('**/api/v1/jobs/job_demo/attempts', (route) => route.fulfill({ json: { attempts: [] } }))
  let submissions = 0
  await page.route('**/api/v1/jobs', (route) => {
    submissions++
    expect(route.request().postDataJSON()).toMatchObject({ scheduled_at: '2027-03-14T02:30:00-06:00' })
    expect(route.request().headers()['idempotency-key']).toBe('alpha')
    return route.fulfill({ status: 202, json: { job_id: job.id, status: 'QUEUED' } })
  })
  await page.goto('/')
  await page.getByRole('button', { name: 'Submit job' }).click()
  const key = page.getByLabel('Idempotency key')
  await key.fill(' alpha ')
  await page.getByLabel('Schedule (RFC3339)').fill('2027-03-14T02:30:00-06:00')
  await page.getByRole('button', { name: 'Submit', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('without whitespace')
  expect(submissions).toBe(0)
  await key.fill('alpha')
  await page.getByRole('button', { name: 'Submit', exact: true }).click()
  await expect(page.getByRole('region', { name: 'Job details' })).toBeVisible()
  expect(submissions).toBe(1)
})
