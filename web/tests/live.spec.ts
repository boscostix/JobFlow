import { randomUUID } from 'node:crypto'
import { expect, test } from '@playwright/test'

test.skip(!process.env.JOBFLOW_E2E_URL, 'Requires a running JobFlow dashboard, API, and workers.')

test('operates the real queue through the dashboard and API', async ({ page, request }) => {
  await page.goto('/')
  const submit = async (payload: string, future = false) => {
    await page.getByRole('button', { name: 'Submit job' }).click()
    await page.getByLabel('Payload').fill(payload)
    if (future) {
      const time = new Date(Date.now() + 60 * 60 * 1000)
      await page.getByLabel('Schedule (RFC3339)').fill(time.toISOString())
    }
    await page.getByLabel('Idempotency key').fill(randomUUID())
    const response = page.waitForResponse((response) =>
      response.url().endsWith('/api/v1/jobs') && response.request().method() === 'POST')
    await page.getByRole('button', { name: 'Submit', exact: true }).click()
    const submitted = await response
    expect(submitted.status()).toBe(202)
    return (await submitted.json()).job_id as string
  }
  const detail = page.getByRole('region', { name: 'Job details' })
  const id = await submit('{"a":9223372036854775806,"b":1}')
  await expect(detail.getByText('Completed', { exact: true }).first()).toBeVisible({ timeout: 12_000 })
  await expect(detail.getByText(/"sum": 9223372036854775807/).first()).toBeVisible()
  await expect(detail.getByText('Token 1')).toBeVisible({ timeout: 12_000 })
  const stored = await request.get(`/api/v1/jobs/${id}`)
  expect(stored.status()).toBe(200)
  expect(await stored.text()).toContain('9223372036854775807')
  await page.getByLabel('Search').fill(id)
  await page.getByLabel('Status', { exact: true }).selectOption('COMPLETED')
  await page.getByRole('button', { name: 'Apply filters' }).click()
  await expect(page.getByRole('region', { name: 'Job list' }).getByRole('button', { name: id })).toBeVisible()

  const future = await submit('{"a":1,"b":2}', true)
  await expect(detail.getByText('Scheduled', { exact: true })).toBeVisible()
  await detail.getByRole('button', { name: 'Cancel job' }).click()
  await expect(detail.getByText('Cancelled', { exact: true })).toBeVisible()
  await expect(detail.getByText('No attempts yet.')).toBeVisible()
  const denied = await request.post(`/api/v1/jobs/${future}/retry`)
  expect(denied.status()).toBe(409)

  await submit('{"a":9223372036854775807,"b":1}')
  await expect(detail.getByText('Dead letter', { exact: true })).toBeVisible({ timeout: 12_000 })
  await expect(detail.getByText('integer addition overflow').first()).toBeVisible()
  await detail.getByRole('button', { name: 'Retry job' }).click()
  await expect(detail.getByText('Token 2')).toBeVisible({ timeout: 12_000 })
  await expect(detail.getByText('Token 1')).toBeVisible()

  await page.getByRole('button', { name: 'Workers' }).click()
  await expect(page.getByRole('heading', { name: 'Workers' })).toBeVisible()
  await expect(page.getByText('Online', { exact: true }).first()).toBeVisible()
})
