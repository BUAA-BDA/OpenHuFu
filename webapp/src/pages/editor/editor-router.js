import {createRouter, createWebHashHistory} from 'vue-router'

import EditorMain from './editor-main';

export default createRouter({
    history: createWebHashHistory(),
    routes: [
        {
            path: '/',
            name: 'editor',
            component: EditorMain
        }
    ]
});