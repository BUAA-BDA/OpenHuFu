<template>
  <div class="datasource">
    <el-form :model="form" label-width="130px" :span="8">
      <el-form-item :label="$t('datasource.datasourceType')">
        <el-select v-model="form.datasourceType"
                   :placeholder="$t('datasource.chooseDatasourceType')"
                    clearable
                    filterable
                    >
          <el-option label="Hive" value="Hive"/>
          <el-option label="Kylin" value="Kylin"/>
          <el-option label="Mysql" value="Mysql"/>
          <el-option label="PostGIS" value="PostGIS"/>
          <el-option label="PostgreSQL" value="PostgreSQL" selected/>
          <el-option label="SQLLite" value="SQLLite"/>
          <el-option label="CSV" value="CSV"/>
          <el-option label="JSON" value="JSON"/>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('datasource.jdbcUrl')">
        <el-input v-model="form.jdbcUrl"/>
      </el-form-item>
      <el-form-item :label="$t('datasource.username')">
        <el-input class="auth" v-model="form.username"/>
      </el-form-item>
      <el-form-item :label="$t('datasource.password')">
        <el-input class="auth" v-model="form.password" type="password" show-password/>
      </el-form-item>

      <el-form-item>
        <el-button @click="onSubmit">{{ $t("datasource.testConnection") }}</el-button>
        <el-button type="primary" @click="onSubmit">{{ $t("datasource.confirm") }}</el-button>
      </el-form-item>

      <el-form-item :label="$t('datasource.tables')">
        <el-tree-select
            v-model="form.tables"
            :data="data"
            multiple
            :render-after-expand="false"
            show-checkbox
            check-strictly
            check-on-click-node
            :placeholder="$t('datasource.chooseTables')"
        />
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import {reactive, ref} from 'vue'

export default {
  name: 'datasourcePage',
  components: {
    // eslint-disable-next-line
  },
  setup() {
  },
  data() {
    return {
      form: reactive({
        datasourceType: 'Hive',
        jdbcUrl: 'jdbc:postgresql://localhost:13101/postgres',
        username: 'postgres',
        password: 'onedb',
        tables: ref()
      }),
      data: [
        {
          value: '1',
          label: 'student',
          children: [],
        },
        {
          value: '2',
          label: 'taxi',
          children: [],
        },
        {
          value: '3',
          label: 'lineitem',
          children: [],
        },
        {
          value: '4',
          label: 'partsupp',
          children: [],
        },
        {
          value: '5',
          label: 'nation',
          children: [],
        },
        {
          value: '6',
          label: 'part',
          children: [],
        },
        {
          value: '7',
          label: 'supplier',
          children: [],
        },
        {
          value: '8',
          label: 'orders',
          children: [],
        },
        {
          value: '9',
          label: 'region',
          children: [],
        },
        {
          value: '10',
          label: 'customer',
          children: [],
        },
      ]
    }
  },
  methods: {
    onSubmit() {
      console.log('submit!')
    }
  }
}
</script>

<style>

.datasource {
  margin-top: 20px;
  margin-left: 5px;
  width: 50%;
}

.el-select {
  width: 400px;
}

.auth {
  width: 300px;
}
</style>