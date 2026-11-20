<template>
  <section class="page" data-module="dlq_review">
    <header class="page-head">
      <div>
        <h2>死信复核数据包</h2>
        <p class="page-desc">
          从待审死信导出带原始载荷与处置历史的复核包，并发导入只保留一份有效复核，
          串联接入质量校验并在质检待办区分可结案字段证据与待复验外部结论。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="export-package" type="button" @click="onExport">
          导出复核数据包
        </button>
        <label class="btn" data-testid="import-package">
          导入复核包
          <input ref="fileInput" type="file" accept="application/json,.json" hidden @change="onImportFile" />
        </label>
        <button class="btn" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <p v-if="message" class="page-foot" :class="{ 'error-text': hasError }">{{ message }}</p>

    <h3 class="section-title">复核包（同一死信仅一份有效，旧包可回查载荷版本）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>复核包编号</th>
          <th>来源死信数</th>
          <th>包状态</th>
          <th>载荷版本</th>
          <th>导入时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="pkg in packages" :key="String(pkg.package_no)" :data-testid="`pkg-${pkg.package_no}`">
          <td>{{ pkg['复核包编号'] }}</td>
          <td>{{ pkg['来源死信数'] }}</td>
          <td>
            <span :class="['status-tag', pkg['包状态'] === '已导入' ? 'ok' : 'muted']">
              {{ pkg['包状态'] }}
            </span>
          </td>
          <td>{{ pkg['载荷版本'] }}</td>
          <td>{{ pkg['导入时间'] }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`qc-${pkg.package_no}`" @click="onQualityCheck(String(pkg.package_no))">
              批量质量校验
            </button>
          </td>
        </tr>
        <tr v-if="!packages.length">
          <td colspan="6" class="empty-state">暂无复核包，先从死信队列管理导出或直接导出待审死信</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">质检待办（字段证据可结案 · 外部结论待复验）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>待办编号</th>
          <th>死信</th>
          <th>来源复核包</th>
          <th>质检结论</th>
          <th>待办状态</th>
          <th>证据可用性</th>
          <th>字段证据</th>
          <th>待复验外部结论</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="todo in todos" :key="String(todo.id)" :data-testid="`todo-${todo.id}`">
          <td>{{ todo['待办编号'] }}</td>
          <td>{{ todo['记录编号'] }}</td>
          <td>{{ todo['来源复核包'] }}</td>
          <td>{{ todo['质检结论'] }}</td>
          <td>
            <span :class="['status-tag', todoClass(todo)]">{{ todo['待办状态'] }}</span>
          </td>
          <td>{{ todo['证据可用性'] }}</td>
          <td>
            <span v-if="todo.字段证据?.length" class="evidence ok-evidence">
              {{ todo.字段证据.length }} 条可结案证据
            </span>
            <span v-else class="evidence no-evidence">无</span>
          </td>
          <td>
            <span v-if="todo.外部结论?.length" class="evidence pending-evidence">
              {{ todo.外部结论.length }} 条待复验
            </span>
            <span v-else class="evidence no-evidence">无</span>
          </td>
          <td class="row-actions">
            <button
              v-if="todo.待办状态 !== '已结案'"
              class="link"
              type="button"
              :disabled="!canClose(todo)"
              :data-testid="`close-${todo.id}`"
              @click="onClose(todo.id)"
            >
              结案
            </button>
            <span v-else class="muted">已结案</span>
          </td>
        </tr>
        <tr v-if="!todos.length">
          <td colspan="9" class="empty-state">暂无质检待办，对复核包执行批量质量校验后生成</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  closeQualityTodo,
  exportReviewPackage,
  importReviewPackage,
  listQualityTodos,
  listReviewPackages,
  runQualityCheck,
} from '@/api/backend-service'
import type { QualityTodo, ReviewPackage } from '@/api/backend-service'

const packages = ref<Array<Record<string, unknown>>>([])
const todos = ref<QualityTodo[]>([])
const message = ref('')
const hasError = ref(false)
const fileInput = ref<HTMLInputElement | null>(null)

function notify(text: string, isError = false) {
  message.value = text
  hasError.value = isError
}

function canClose(todo: QualityTodo): boolean {
  // 仅当存在当前有效包的字段证据且无阻断待复验时可结案
  return todo.证据可用性 === '可结案' && todo.待办状态 === '待处理'
}

function todoClass(todo: QualityTodo): string {
  if (todo.待办状态 === '已结案') return 'ok'
  if (todo.待办状态 === '待复验') return 'warn'
  return 'muted'
}

async function reload() {
  try {
    const [pkgResp, todoResp] = await Promise.all([listReviewPackages(), listQualityTodos()])
    packages.value = pkgResp.items
    todos.value = todoResp.items
  } catch (error) {
    notify(error instanceof Error ? error.message : '数据读取失败', true)
  }
}

async function onExport() {
  try {
    // null 表示导出全部待审死信
    const pkg: ReviewPackage = await exportReviewPackage(null)
    notify(`复核包 ${pkg.package_no} 已导出并下载，含 ${pkg.item_count} 条失败复核记录`)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '导出失败', true)
  }
}

async function onImportFile(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  try {
    const text = await file.text()
    const packageObj = JSON.parse(text)
    const result = (await importReviewPackage(packageObj)) as { message: string }
    notify(result.message)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '导入失败，请确认是有效的复核包 JSON', true)
  } finally {
    input.value = ''
  }
}

async function onQualityCheck(packageNo: string) {
  try {
    const result = (await runQualityCheck(packageNo)) as { message: string }
    notify(result.message)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '质量校验失败', true)
  }
}

async function onClose(todoId: number) {
  try {
    const result = (await closeQualityTodo(todoId)) as { message: string }
    notify(result.message)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '结案失败', true)
  }
}

onMounted(reload)
</script>

<style scoped>
.section-title {
  margin: 20px 0 8px;
  font-size: 15px;
}
.status-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
}
.status-tag.ok {
  background: #e6f7ec;
  color: #1a7f37;
}
.status-tag.warn {
  background: #fff4e0;
  color: #b26a00;
}
.status-tag.muted {
  background: #eef0f2;
  color: #667085;
}
.evidence {
  font-size: 12px;
}
.ok-evidence {
  color: #1a7f37;
}
.pending-evidence {
  color: #b26a00;
}
.no-evidence {
  color: #98a2b3;
}
.muted {
  color: #98a2b3;
}
</style>
