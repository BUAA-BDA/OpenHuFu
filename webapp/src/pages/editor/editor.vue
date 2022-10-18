<template>
  <editor-main></editor-main>
</template>

<script>
import router from './editor-router';
import EditorMain from './editor-main';

export default {
  name: 'EditorPage',
  components: {
    // eslint-disable-next-line
    EditorMain,
  },
  router
}
</script>