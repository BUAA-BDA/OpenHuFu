import { createApp } from 'vue'
import App from './App.vue'
import router from './router/index'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import {FontAwesomeIcon} from '@fortawesome/vue-fontawesome'
import Cookie from 'js-cookie';
import i18n from "@/plugins/i18n";
import * as ElementPlusIconsVue from '@element-plus/icons-vue'



const app = createApp(App)

for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
    app.component(key, component)
}
app.use(router)
app.use(ElementPlus)
app.use(i18n)

app.component('font-awesome-icon', FontAwesomeIcon)
app.mount('#app')