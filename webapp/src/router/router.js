import { createRouter, createWebHashHistory } from "vue-router";

const routes = [
  {
    name: "dash-board",
    path: "/",
    component: () => import("@/pages/dash-board/DashBoard.vue"),
  },
  {
    name: "schema-manager",
    path: "/schema-manager",
    component: () => import("@/pages/schema-manager/SchemaManager.vue"),
  },
  {
    name: "sql-panel",
    path: "/sql-panel",
    component: () => import("@/pages/sql-panel/SqlPanel.vue"),
  },
];

const router = createRouter({
  history: createWebHashHistory(),
  routes,
});

export default router;
