<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="meta.key === 'merge_plan'"
              class="link"
              type="button"
              :data-testid="`inputs-${row.id}`"
              @click="openMergeInputsPanel(row)"
            >
              合并输入
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <section
      v-if="meta.key === 'merge_plan' && mergePanelRow && mergeInputs"
      class="merge-panel"
      data-testid="merge-input-panel"
    >
      <header class="page-head">
        <div>
          <h3>合并输入清单 · {{ mergePanelRow['计划编号'] }}</h3>
          <p class="page-desc">
            批式来源 {{ mergeInputs.manifest.批式来源 || '—' }} · 流式来源
            {{ mergeInputs.manifest.流式来源 || '—' }}（已选输入审计只读）
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" data-testid="refresh-inputs" type="button" @click="refreshMergeInputs">
            刷新清单
          </button>
          <button class="btn" data-testid="open-preview" type="button" @click="openPreview">
            打开预览
          </button>
          <button class="btn primary" data-testid="publish-merge" type="button" @click="publishNow">
            执行发布
          </button>
        </div>
      </header>

      <div class="merge-sides">
        <article v-for="(side, label) in mergeInputs.manifest.输入" :key="label" class="stat-card">
          <span class="stat-label">{{ label }}输入 · {{ side.来源名称 || '未声明' }}</span>
          <strong class="stat-value">{{ side.状态 }}</strong>
          <p class="merge-side-line">已核验版本：{{ side.已核验版本 || '—' }}</p>
          <p class="merge-side-line">覆盖键：{{ side.覆盖键.length ? side.覆盖键.join('、') : '—' }}</p>
          <p class="merge-side-line">行数：{{ side.行数 }} · {{ side.来源说明 }}</p>
        </article>
      </div>

      <p v-if="mergeInputs.manifest.留待核对?.length" class="merge-pending">
        留待核对（归属未确认，本计划不读取）：
        <span v-for="item in mergeInputs.manifest.留待核对" :key="`${item.侧}-${item.版本}`" class="legend-item">
          [{{ item.侧 }}] {{ item.来源名称 }} @ {{ item.版本 }} — {{ item.原因 }}
        </span>
      </p>

      <div v-if="mergeInputs.manifest.预览证明" class="merge-proof" data-testid="merge-proof">
        <span class="stat-label">已打开预览的来源证明（{{ mergeInputs.manifest.预览证明.证明编号 }}）</span>
        <p class="merge-side-line">
          分片版本 {{ mergeInputs.manifest.预览证明.分片版本 || '—' }}，覆盖键
          {{ mergeInputs.manifest.预览证明.分片覆盖键.join('、') || '—' }}；
          关联版本 {{ mergeInputs.manifest.预览证明.关联版本 || '—' }}，覆盖键
          {{ mergeInputs.manifest.预览证明.关联覆盖键.join('、') || '—' }}
        </p>
      </div>

      <div class="merge-proof" data-testid="merge-publish">
        <span class="stat-label">发布回写</span>
        <p class="merge-side-line">
          已发布分片版本：{{ mergeInputs.manifest.已发布分片版本 || '—' }} · 已发布关联版本：{{
            mergeInputs.manifest.已发布关联版本 || '—'
          }}
        </p>
        <p class="merge-side-line">
          待补范围：{{ mergeInputs.manifest.待补范围?.length ? mergeInputs.manifest.待补范围.join('、') : '无' }}
        </p>
        <p v-for="record in mergeInputs.manifest.发布记录" :key="`${record.结果批次}-${record.时间}`" class="merge-side-line">
          {{ record.结果 }} · 分片 {{ record.分片版本 || '—' }} · 关联 {{ record.关联版本 || '—' }} · 待补
          {{ record.待补范围.length ? record.待补范围.join('、') : '无' }}
        </p>
      </div>

      <p v-if="mergeMessage" class="error-text">{{ mergeMessage }}</p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  loadMergeInputs,
  moduleMeta,
  openMergePreview,
  publishMerge,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow, MergeInputsResult } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
// 合并输入查询面板（仅 merge_plan 模块）：审计只读查看已选输入与发布回写
const mergePanelRow = ref<EntryRow | null>(null)
const mergeInputs = ref<MergeInputsResult | null>(null)
const mergeMessage = ref('')
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function openMergeInputsPanel(row: EntryRow) {
  mergePanelRow.value = row
  await refreshMergeInputs()
}

async function refreshMergeInputs() {
  if (!mergePanelRow.value) return
  mergeMessage.value = ''
  try {
    mergeInputs.value = await loadMergeInputs(Number(mergePanelRow.value.id))
  } catch (error) {
    mergeMessage.value = error instanceof Error ? error.message : '合并输入清单读取失败'
  }
}

async function openPreview() {
  if (!mergePanelRow.value) return
  mergeMessage.value = ''
  try {
    const result = await openMergePreview(Number(mergePanelRow.value.id))
    if (!result.ok) throw new Error(result.reason ?? '打开预览失败')
    mergeInputs.value = result
  } catch (error) {
    mergeMessage.value = error instanceof Error ? error.message : '打开预览失败'
  }
}

async function publishNow() {
  if (!mergePanelRow.value) return
  mergeMessage.value = ''
  try {
    const result = await publishMerge(Number(mergePanelRow.value.id))
    mergeInputs.value = result
    if (!result.ok) {
      mergeMessage.value = result.reason ?? '发布暂缓'
    }
    await reload()
  } catch (error) {
    mergeMessage.value = error instanceof Error ? error.message : '发布失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
