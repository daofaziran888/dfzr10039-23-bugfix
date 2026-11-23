import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 投递回执与抽样工作台
// ---------------------------------------------------------------------------

export type ReceiptSample = {
  plan_id: string
  target_batch: string
  target_system: string
  receipt_id: string
  data_version: string
  delivery_task_id: string
  sample_size: number
  check_result: string
  status: string
}

export type ReceiptDetail = {
  batch_id: string
  batch_version: string
  target_system: string
  receipt_id: string
  receipt_status: string
  reconciliation_status: string
  claim_status: string
  reissue_count: number
  intake_count: number
  received_at: string
  delivered_row_count: number
  task_id: string
  task_count: number
  pending_stage: string
  sample: ReceiptSample | null
}

export type DeliveryTrailItem = {
  target_system: string
  batch_version: string
  receipt_id: string
  receipt_status: string
  reissue_count: number
  task_id: string
  sample_plan_id: string
  data_version: string
}

export type SampleWorkbenchItem = {
  target_system: string
  receipt_id: string
  data_version: string
  plan_id: string
  sample_size: number
  check_result: string
  status: string
}

function deliveryBase(batchId: string, version: string): string {
  return `/api/delivery/${encodeURIComponent(batchId)}/${encodeURIComponent(version)}`
}

export async function loadReceiptDetail(
  batchId: string,
  version: string,
  target: string,
): Promise<ReceiptDetail | null> {
  const response = await request(
    `${deliveryBase(batchId, version)}/targets/${encodeURIComponent(target)}`,
  )
  if (response.status === 404) return null
  if (!response.ok) throw new Error('投递回执详情读取失败')
  return await response.json()
}

export async function loadDeliveryTrail(
  batchId: string,
  version: string,
): Promise<DeliveryTrailItem[]> {
  const response = await request(`${deliveryBase(batchId, version)}/trail`)
  if (!response.ok) throw new Error('投递轨迹读取失败')
  return await response.json()
}

export async function loadSampleWorkbench(
  batchId: string,
  version: string,
): Promise<SampleWorkbenchItem[]> {
  const response = await request(`${deliveryBase(batchId, version)}/samples`)
  if (!response.ok) throw new Error('抽样工作台读取失败')
  return await response.json()
}

export async function runDeliveryAction(
  batchId: string,
  version: string,
  target: string,
  action: string,
  options: { interruption?: string; token?: string } = {},
): Promise<ActionResult> {
  const response = await request(
    `${deliveryBase(batchId, version)}/targets/${encodeURIComponent(target)}/actions`,
    {
      method: 'POST',
      body: JSON.stringify({
        action,
        submission_token: options.token ?? '',
        interrupt_before: options.interruption ?? null,
      }),
    },
  )
  return await response.json()
}
