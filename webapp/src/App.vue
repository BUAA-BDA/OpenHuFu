<template>
  <el-config-provider :locale="locale">
    <div id="app">
      <router-view></router-view>
    </div>
  </el-config-provider>
</template>

<script>
import { computed, defineComponent } from 'vue'
import { ElConfigProvider } from 'element-plus'

import zhCn from 'element-plus/lib/locale/lang/zh-cn'
import en from 'element-plus/lib/locale/lang/en'
import { getLocalLanguage } from './plugins/i18n'

export default defineComponent({
  name: "App",
  components: {
    ElConfigProvider,
  },
  setup() {
    const localLanguage = computed(() => {
      switch (getLocalLanguage()) {
        case 'en':
          return en
        case 'zh':
        default:
          return zhCn
      }
    })
    return {
      locale: localLanguage,
    }
  },
})
</script>

<style>
#app {
  height: 100vh;
  width: 100%;
}
</style>