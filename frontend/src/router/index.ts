import { createRouter, createWebHistory } from 'vue-router'

import DeliveryReceiptView from '@/views/DeliveryReceiptView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/delivery_receipt', name: 'delivery_receipt', component: DeliveryReceiptView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
