import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import DlqReviewView from '@/views/DlqReviewView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/dlq_review', name: 'dlq-review', component: DlqReviewView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
