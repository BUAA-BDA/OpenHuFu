<template>
  <div class="layout">
    <layout-header></layout-header>
    <div class="layout-content">
      <router-view></router-view>
    </div>
    <div class="layout-footer">
    </div>
  </div>
</template>

<script>
import LayoutHeader from './layout-header';

export default {
  name: 'VueLayout',
  components: {
    LayoutHeader,
  },
}
</script>
