import { createRouter, createWebHistory } from "vue-router";

import layout from '@/layout/layout';

const Overview = () => import('@/pages/overview/overview');
const Editor = () => import('@/pages/editor/editor');
const Operation = () => import('@/pages/operation/operation');
const Datasource = () => import('@/pages/datasource/datasource');
const Setting = () => import('@/pages/setting/setting');

const routes = [
  {
    path: '/',
    name: 'layout',
    component: layout,
    children: [
      {
        path: 'overview',
        name: 'overview',
        component: Overview
      },
      {
        path: 'editor',
        name: 'editor',
        component: Editor
      },
      {
        path: 'operation',
        name: 'operation',
        component: Operation
      },
      {
        path: 'datasource',
        name: 'datasource',
        component: Datasource
      },
      {
        path: 'setting',
        name: 'setting',
        component: Setting
      },
    ],
  }
];

const router = createRouter({
  history: createWebHistory(),
  mode: 'history',
  routes,
});

export default router;
