<template>
  <div class="h-6"/>
  <el-menu
      :default-active="$route.path"
      :router="true"
      class="el-menu"
      mode="horizontal"
      :ellipsis="false"
      background-color="#202529"
      text-color="#ffffff"
      active-text-color="#ffd04b"
  >
    <img class="logo" src="../assets/images/hufu-logo.svg" alt="金融大数据" />
    <el-menu-item :index="navItem.path"  v-for="navItem in navItems" :key="navItem.name" class="flex-left">
      <template #title>
        <el-icon>
          <component :is="navItem.icon"/>
        </el-icon>
        <span>{{ $t(navItem.name) }}</span>
      </template>
    </el-menu-item>

    <div class="flex-right"/>
    <el-sub-menu index="6">
      <template #title>
        <el-icon><Reading /></el-icon>
        {{ $t("language.locale") }}
      </template>
      <el-menu-item index="6-1" @click="changeLanguage('zh')">中文</el-menu-item>
      <el-menu-item index="6-2" @click="changeLanguage('en')">English</el-menu-item>
    </el-sub-menu>
  </el-menu>
</template>

<script>
import {defineComponent} from 'vue'
import {Menu, Edit, Operation, Compass, Setting, Reading} from '@element-plus/icons-vue'
import {ElMessage} from 'element-plus'
import router from '../router'

const handleCommand = (command) => {
  ElMessage(`click on item ${command}`)
}

export default defineComponent({
  components: {
    Menu,
    Edit,
    Operation,
    Compass,
    Setting,
    Reading,
  },
  setup() {
  },
  data: () => {
    return {
      navItems: [
        {
          icon: 'Menu',
          name: 'menu.overview',
          path: '/overview'
        },
        {
          icon: 'Edit',
          name: 'menu.editor',
          path: '/editor'
        },
        {
          icon: 'Operation',
          name: 'menu.operation',
          path: '/operation'
        },
        {
          icon: 'Compass',
          name: 'menu.datasource',
          path: '/datasource'
        },
        {
          icon: 'Setting',
          name: 'menu.setting',
          path: '/setting'
        },

      ],
    };
  },
  methods: {
    navPath(path) {
      if (path) {
        location.hash = path;
      }
    },
    changeLanguage(lang) {
      localStorage.setItem('my_locale', lang)
      location.reload()
    }
  },
  computed: {
    currentNavItem() {
      // console.log('this.$route', this.$route);
      const path = this.$route.path;
      const navItem = this.navItems.find((_navItem) => {
        // console.log('_navItem', _navItem);
        return _navItem.path && path == _navItem.path; //path.indexOf(_navItem.path) > -1;
      });
      // console.log('navItem', navItem);
      return navItem || {};
    }
  },
})
</script>

<style scoped>
.logo{
  width:40px;
  height:40px;
  position: fixed;
  float:left;
  left:10px;
  top:12px;
}
.flex-left {
  left:50px;
}
.flex-right {
  flex-grow: 20;
}
</style>