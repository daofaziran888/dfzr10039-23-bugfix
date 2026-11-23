<template>
  <section class="page" data-module="delivery_receipt">
    <header class="page-head">
      <div>
        <h2>投递回执详情</h2>
        <p class="page-desc">
          按批次版本核对每个目标的入仓次数、收讫凭证与抽样数据版本。
          目标已经接收但本地写回中断时显示“待对账”，恢复后补全为同一份凭证。
        </p>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="refresh">
      <label class="filter-item">
        <span>批次编号</span>
        <input v-model="batchId" data-testid="receipt-batch" placeholder="如 B-A" />
      </label>
      <label class="filter-item">
        <span>批次版本</span>
        <input v-model="version" data-testid="receipt-version" placeholder="如 v1" />
      </label>
      <label class="filter-item">
        <span>目标系统</span>
        <input v-model="target" data-testid="receipt-target" placeholder="如 W1" />
      </label>
      <button class="btn" data-testid="receipt-query" type="submit">查询回执</button>
    </form>

    <div class="page-actions" data-testid="receipt-actions">
      <button class="btn primary" type="button" data-testid="deliver-target" @click="act('投递')">
        投递到目标
      </button>
      <button class="btn" type="button" data-testid="retry-target" @click="act('重试投递')">
        重试投递
      </button>
      <button class="btn" type="button" data-testid="recover-target" @click="act('恢复投递')">
        恢复中断投递
      </button>
      <button class="btn" type="button" data-testid="backfill-receipt" @click="act('补发收执')">
        接收仓库补发收执
      </button>
      <button
        class="btn ghost"
        type="button"
        data-testid="simulate-interrupt"
        @click="act('投递', 'receipt')"
      >
        模拟回执后写回中断
      </button>
    </div>

    <div v-if="detail" class="stat-row" data-testid="receipt-detail">
      <article class="stat-card">
        <span class="stat-label">入仓次数</span>
        <strong class="stat-value" data-testid="intake-count">{{ detail.intake_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">收讫凭证</span>
        <strong class="stat-value" data-testid="receipt-id">{{ detail.receipt_id || '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">投递资格</span>
        <strong class="stat-value" data-testid="claim-status">{{ detail.claim_status }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">补发次数</span>
        <strong class="stat-value" data-testid="reissue-count">{{ detail.reissue_count }}</strong>
      </article>
    </div>

    <table v-if="detail" class="data-table">
      <thead>
        <tr>
          <th>批次版本</th><th>目标系统</th><th>投递任务</th><th>收讫状态</th>
          <th>对账状态</th><th>投递行数</th><th>待对账阶段</th>
        </tr>
      </thead>
      <tbody>
        <tr data-testid="receipt-row">
          <td>{{ detail.batch_version }}</td>
          <td>{{ detail.target_system }}</td>
          <td>{{ detail.task_id || '—' }}</td>
          <td>{{ detail.receipt_status }}</td>
          <td>{{ detail.reconciliation_status }}</td>
          <td>{{ detail.delivered_row_count }}</td>
          <td>{{ detail.pending_stage || '—' }}</td>
        </tr>
      </tbody>
    </table>

    <h3>目标收讫轨迹</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>目标系统</th><th>批次版本</th><th>收讫凭证</th><th>收讫状态</th>
          <th>投递任务</th><th>抽样计划</th><th>样本数据版本</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in trail"
          :key="item.target_system"
          :data-testid="`trail-${item.target_system}`"
        >
          <td>{{ item.target_system }}</td>
          <td>{{ item.batch_version }}</td>
          <td>{{ item.receipt_id }}</td>
          <td>{{ item.receipt_status }}</td>
          <td>{{ item.task_id || '—' }}</td>
          <td>{{ item.sample_plan_id || '—' }}</td>
          <td>{{ item.data_version || '—' }}</td>
        </tr>
        <tr v-if="!trail.length">
          <td colspan="7" class="empty-state">该批次版本还没有向任何目标投递</td>
        </tr>
      </tbody>
    </table>

    <h3>抽样工作台</h3>
    <table class="data-table" data-testid="sample-workbench">
      <thead>
        <tr>
          <th>目标系统</th><th>收讫凭证</th><th>抽样计划</th><th>数据版本</th>
          <th>样本数量</th><th>检验结果</th><th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in samples"
          :key="`${item.target_system}-${item.plan_id}`"
          :data-testid="`sample-${item.target_system}`"
        >
          <td>{{ item.target_system }}</td>
          <td>{{ item.receipt_id }}</td>
          <td>{{ item.plan_id || '—' }}</td>
          <td>{{ item.data_version }}</td>
          <td>{{ item.sample_size }}</td>
          <td>{{ item.check_result || '—' }}</td>
          <td>{{ item.status }}</td>
        </tr>
        <tr v-if="!samples.length">
          <td colspan="7" class="empty-state">尚无有效收讫凭证可供抽样</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span data-testid="receipt-message">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  loadDeliveryTrail,
  loadReceiptDetail,
  loadSampleWorkbench,
  runDeliveryAction,
} from '@/api/backend-service'
import type { DeliveryTrailItem, ReceiptDetail, SampleWorkbenchItem } from '@/api/backend-service'

const batchId = ref('B-A')
const version = ref('v1')
const target = ref('W1')
const detail = ref<ReceiptDetail | null>(null)
const trail = ref<DeliveryTrailItem[]>([])
const samples = ref<SampleWorkbenchItem[]>([])
const message = ref('')
const errorMessage = ref('')

async function refresh() {
  errorMessage.value = ''
  try {
    detail.value = await loadReceiptDetail(batchId.value, version.value, target.value)
    trail.value = await loadDeliveryTrail(batchId.value, version.value)
    samples.value = await loadSampleWorkbench(batchId.value, version.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执详情读取失败'
  }
}

async function act(action: string, interruption?: string) {
  errorMessage.value = ''
  try {
    const result = await runDeliveryAction(
      batchId.value, version.value, target.value, action,
      { interruption, token: `${batchId.value}@${version.value}->${target.value}` },
    )
    message.value = result.message
    if (!result.ok && action !== '模拟回执后写回中断') {
      errorMessage.value = result.message
    }
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '投递动作执行失败'
  }
}

onMounted(refresh)
</script>
