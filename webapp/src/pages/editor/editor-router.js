import {createRouter, createWebHistory} from 'vue-router'

import EditorMain from './editor-main';

export default createRouter({
    history: createWebHistory(),
    routes: [
        {
            path: '/',
            name: 'editor',
            component: EditorMain
        }
    ]
});