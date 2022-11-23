<template>
  <div class="setting">
    <div class="search-bar">
      <div class="search-bar2">
        <a class="my-pad">{{ $t("setting.address") }}:</a>
        <el-input
            v-model="recordAddress"
            :placeholder="$t('operation.pleaseInput')"
            clearable
            style="width:300px;">
        </el-input>
      </div>
      <div class="search-bar2">
        <a class="my-pad">{{ $t("setting.status") }}:</a>
        <el-select
            v-model="recordStatus"
            :placeholder="$t('operation.pleaseSelect')"
            text-align="center"
            style="width:150px;">
          <el-option
              v-for="item in allStatus"
              :key="$t(item.name)"
              :label="$t(item.name)"
              :value="item.value"
          />
        </el-select>
      </div>
      <div class="search-bar2">
        <el-button
            type="primary"
            @click="searchOwner">
          {{ $t('operation.query') }}
        </el-button>
        <el-button
            type="primary"
            @click="clearContextInput">
          {{ $t('operation.reset') }}
        </el-button>
      </div>
    </div>
    <div class="owner-operation">
      <a class="my-pad">{{ $t("setting.newOwner") }}:</a>
      <el-input
          v-model="ownerAddress"
          :placeholder="$t('setting.addAddress')"
          clearable
          class="owner-operation"
          style="width: 250px"
      >
      </el-input>
      <el-button
          type="primary"
          @click="addOwner()"
          v-if="ownerAddress.length !== 0"
      >
        <el-icon><Plus/></el-icon>
      </el-button>
      <el-button v-if="ownerAddress.length === 0" disabled>
        <el-icon><Plus/></el-icon>
      </el-button>
    </div>
    <div>
      <el-table :data="tableData" stripe
                class="result-table"
                style="overflow-y:auto">
        <template v-slot:empty>
          {{ $t('operation.noData') }}
        </template>
        <el-table-column prop="id" :label="$t('setting.id')">
        </el-table-column>
        <el-table-column prop="address" :label="$t('setting.address')">
        </el-table-column>
        <el-table-column :label="$t('setting.status')">
          <template #default="scope">
            <div style="display: flex; align-items: center">
              <el-icon v-if="scope.row.status === 'connected'" style="color: deepskyblue"><Select/></el-icon>
              <el-icon v-if="scope.row.status === 'disconnected'" style="color: brown">
                <CloseBold/>
              </el-icon>
              <span style="margin-left: 10px">{{ formlize1(scope.row) }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column :label="$t('setting.tables')">
          <template #default="scope">
            <div style="color:deepskyblue">
              <span>{{ scope.row.tableNum }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="operation" :label="$t('setting.operation')">
          <template #default="scope">
            <el-button
                type="danger"
                @click="removeOwner(scope.row.address)">
              {{ $t('setting.delete') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
    <div class="search-bar">
      <el-pagination text-align="center" @current-change="currentChange"
                     :current-page="pageNow" :page-size="size" :page-sizes="[10]"
                     layout="total, prev, pager, next, jumper" :total="total">
      </el-pagination>
    </div>
  </div>
</template>

<script>

import axios from "axios";

export default {
  name: 'SettingPage',
  components: {
    // eslint-disable-next-line
  },
  data() {
    return {
      allStatus: [
        {
          name: 'setting.connected',
          value: 'connected'
        },
        {
          name: 'setting.disconnected',
          value: 'disconnected'
        },
      ],
      tableData: [
        //     {
        //   id: 1,
        //   port: 8081,
        //   ip: "127.0.0.1",
        //   status: "disconnected",
        //   tables: 90,
        // }
      ],
      ownerAddress: "",
      recordAddress: "",
      recordStatus: "",
      pageNow: 1,
      size: 10,
      total: 0
    }
  },
  methods: {
    async getLocalTables(owner) {
      let x;
      await axios
          .get("/user/owners/" + owner).then((response) => {
            x = response.data.length;
          })
      return x
    },
    removeOwner(endpoint) {
      axios
          .delete("/user/owners/" + endpoint)
          .then(() => {
            this.searchOwner();
          })
          .catch((err) => {
            console.log(err);
            window.alert(`Fail to connect ${endpoint}`);
          });
    },
    addOwner() {
      axios
          .post("/user/owners", {value: this.ownerAddress})
          .then(() => {
            this.searchOwner();
            this.ownerAddress = "";
          })
          .catch(() => {
            window.alert(`Fail to connect ${this.ownerAddress}`);
          });
      this.ownerAddress = ""
    },
    formlize1(row) {
      let key = row.status;
      for (let item of this.allStatus) {
        if (key === item.value) {
          return this.$t(item.name);
        }
      }
    },
    searchOwner() {
      axios
          .post("/owner/searchowner", {
            context: (this.recordAddress.length === 0) ? null : this.recordAddress,
            status: (this.recordStatus.length === 0) ? null : this.recordStatus,
            pageId: this.pageNow,
            pageSize: this.size
          })
          .then((response) => {
            this.tableData = response.data.data;
            this.total = response.data.pagination.total;
          })
          .catch((err) => {
            console.log(err);
          });
    },
    currentChange(val) {
      this.pageNow = val;
      this.searchOwner();
    },
    clearContextInput() {
      this.recordAddress = ""
      this.recordStatus = ""
      this.pageNow = 1
      this.searchOwner()
    }
  },
  mounted() {
    this.searchOwner()
  }
}
</script>

<style lang="css">
.my-pad {
  padding-right: 20px;
}

.setting {
  margin-left: 20px;
  margin-right: 20px;
}

.search-bar {
  display: flex;
  justify-content: space-around;
  align-items: center;
  height: 50px;
  width: 100%;
  background-color: white;
  padding: 5px 10px;
}

.search-bar2 {
  display: flex;
  justify-content: space-evenly;
  align-items: center;
  height: 50px;
  background-color: white;
  padding: 2px 4px;
}

.owner-operation {
  justify-content: flex-start;
  align-items: center;
}
</style>