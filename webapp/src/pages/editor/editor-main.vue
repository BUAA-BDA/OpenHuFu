<template>
  <el-dialog
      v-model="dialogVisible"
      title="Change desensitize method"
      width="30%"
      :before-close="handleClose"
  >
    <el-form label-width="auto" label-position="left" style="margin: 10% 12% 0 12%;">
      <el-form-item :label="$t('editor.tableName')">
        <el-input v-model="tableName" disabled/>
      </el-form-item>
      <el-form-item :label="$t('editor.colName')">
        <el-input v-model="column.name" disabled/>
      </el-form-item>
      <el-form-item :label="$t('editor.colType')">
        <el-input v-model="column.type" disabled/>
      </el-form-item>

      <el-form-item :label="$t('editor.colSensitivity')">
        <el-select v-model="column.desensitize.sensitivity" class="m-2" placeholder="Select">
          <el-option
              v-for="item in sensitivityOptions"
              :key="item.value"
              :label="item.label"
              :value="item.value"
          />
        </el-select>
      </el-form-item>
      <el-form-item v-if="column.desensitize.sensitivity == 'SENSITIVE'" :label="$t('editor.methodType')">
        <el-select v-model="column.desensitize.method.type" class="m-2" placeholder="Select">
          <el-option
              v-for="item in methodOptions[column.type]"
              :key="item.value"
              :label="item.label"
              :value="item.value"
          />
        </el-select>
      </el-form-item>

      <div v-if="column.desensitize.sensitivity == 'SENSITIVE'">
        <el-form-item v-if="column.desensitize.method.type == 'MASK'" :label="$t('editor.mask.begin')">
          <el-input type="number" v-model="mask.begin"></el-input>
        </el-form-item>
        <el-form-item v-if="column.desensitize.method.type == 'MASK'" :label="$t('editor.mask.end')">
          <el-input type="number" v-model="mask.end"></el-input>
        </el-form-item>
        <el-form-item v-if="column.desensitize.method.type == 'MASK'" :label="$t('editor.mask.str')">
          <el-input v-model="mask.str"></el-input>
        </el-form-item>

        <el-form-item v-if="column.desensitize.method.type == 'REPLACE'" :label="$t('editor.replace.fromStr')">
          <el-input v-model="replace.fromStr"></el-input>
        </el-form-item>
        <el-form-item v-if="column.desensitize.method.type == 'REPLACE'" :label="$t('editor.replace.toStr')">
          <el-input v-model="replace.toStr"></el-input>
        </el-form-item>

        <el-form-item v-if="column.desensitize.method.type == 'NUMBER_FLOOR'" :label="$t('editor.number_floor.place')">
          <el-input type="number" v-model="number_floor.place"></el-input>
        </el-form-item>

        <el-form-item v-if="column.desensitize.method.type == 'DATE_FLOOR'" :label="$t('editor.date_floor.floor')">
          <el-select v-model="date_floor.floor" class="m-2" placeholder="Select">
            <el-option
                v-for="item in dateFloorOptions[column.type]"
                :key="item.value"
                :label="item.label"
                :value="item.value"
            />
          </el-select>
        </el-form-item>
      </div>
    </el-form>

    <template #footer>
      <span class="dialog-footer">
        <el-button @click="handleClose" type="primary">{{$t('operation.updateDesensitization')}}</el-button>
      </span>
    </template>

  </el-dialog>
  <splitpanes class="app-container" id="app">
    <pane class="left-pane">
      <el-scrollbar>
        <el-header class="top-bar connect-info" v-if="connectState == 0">
          <span>
            <font-awesome-icon icon="circle-exclamation" class="prefix-icon" />
            {{ $t("editor.disConnected") }}
          </span>
        </el-header>
        <el-header class="top-bar connect-info" v-if="connectState == 1">
          <span>
            <font-awesome-icon
                icon="spinner"
                class="prefix-icon rotate-element"
            />
            {{ $t("editor.connected") }}
          </span>
        </el-header>
        <el-header class="top-bar connect-info" v-if="connectState == 2">
          <span>
            <font-awesome-icon
                icon="arrows-rotate"
                class="prefix-icon icon-button"
                @click="refreshAll()"
            />
            {{ $t("editor.connected") }}
          </span>
        </el-header>
        <el-header class="top-bar connect-info" v-if="connectState == 3">
          <span>
            <font-awesome-icon icon="circle-exclamation" class="prefix-icon" />
            {{ $t("editor.failToConnectBackend") }}
          </span>
        </el-header>
        <el-collapse class="collapse-panel">
          <el-collapse-item>
            <template #title>
              <div class="collapse-title">
                <font-awesome-icon icon="users" class="prefix-icon" />
                <span>{{ $t("editor.owners") }}</span>
              </div>
            </template>
            <div v-if="connectState != 2" class="collapse-placeholder">
              <span> {{ $t("editor.disConnected") }} </span>
            </div>
            <div
                v-if="connectState == 2 && owners.length == 0"
                class="collapse-placeholder"
            >
              <span> {{ $t("editor.noOwner") }}</span>
            </div>
            <div
                class="collapse-content"
                v-if="connectState == 2 && owners.length != 0"
            >
              <el-tree :data="ownerTree" node-key="id" class="structure-tree">
                <template #default="{ node, data }">
                  <span
                      v-if="node.level != 3"
                      :class="node.level == 1 ? 'tree-entry' : ''"
                  >
                    <span>
                      <font-awesome-icon
                          v-if="node.level == 1"
                          icon="user"
                          class="prefix-icon"
                      ></font-awesome-icon>
                      <font-awesome-icon
                          v-if="node.level == 2"
                          icon="table"
                          class="prefix-icon"
                      ></font-awesome-icon>
                      <span> {{ node.label }} </span>
                    </span>
                    <font-awesome-icon
                        v-if="node.level == 1"
                        icon="trash-can"
                        @click="removeOwner(node.label)"
                    ></font-awesome-icon>
                  </span>
                  <div v-else>
                    <font-awesome-icon
                        v-if="data.modifier == 'PUBLIC'"
                        icon="unlock"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-else
                        icon="lock"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <span>
                      {{ data.name }}
                      <el-tag
                          class="ml-2 info-tag"
                          type="info"
                          effect="plain"
                          round
                      >
                        {{ data.type }}
                      </el-tag>
                      <el-tag class="ml-2 info-tag" effect="plain" round>
                        {{ data.modifier }}
                      </el-tag>
                    </span>
                  </div>
                </template>
              </el-tree>
            </div>
            <div v-if="connectState == 2" class="group-form">
              <el-row justify="space-between" class="form-line">
                <el-col :span="20">
                  <el-input
                      v-model="ownerAddress"
                      :placeholder="$t('editor.newOwner')"
                      clearable
                      class="line-input"
                  >
                  </el-input>
                </el-col>
                <el-col :span="4">
                  <el-button
                      type="primary"
                      @click="addOwner()"
                      v-if="ownerAddress != ''"
                  >
                    <font-awesome-icon icon="plus" />
                  </el-button>
                  <el-button v-if="ownerAddress == ''" disabled>
                    <font-awesome-icon icon="plus" />
                  </el-button>
                </el-col>
              </el-row>
            </div>
          </el-collapse-item>
        </el-collapse>
        <el-collapse class="collapse-panel">
          <el-collapse-item>
            <template #title>
              <div class="collapse-title">
                <font-awesome-icon icon="table-list" class="prefix-icon" />
                <span> {{ $t("editor.globalTables") }}</span>
              </div>
            </template>
            <div v-if="connectState != 2" class="collapse-placeholder">
              <span> {{ $t("editor.disConnected") }}</span>
            </div>
            <div
                v-if="connectState == 2 && globalTables.length == 0"
                class="collapse-placeholder"
            >
              <span> {{ $t("editor.noGlobalTable") }} </span>
            </div>
            <el-tree
                v-if="connectState == 2 && globalTables.length != 0"
                :data="globalTableTree"
                node-key="id"
                class="structure-tree"
            >
              <template #default="{ node, data }">
                <div v-if="node.level == 1" class="tree-entry">
                  <span>
                    <font-awesome-icon
                        v-if="node.level == 1"
                        icon="table"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <span> {{ node.label }} </span>
                  </span>
                  <font-awesome-icon
                      icon="trash-can"
                      class="tail-button"
                      @click="delGlobalTable(node.label)"
                  ></font-awesome-icon>
                </div>
                <span v-else>
                  <div>
                    <font-awesome-icon
                        v-if="data.icon == 'owner'"
                        icon="user"
                        class="prefix-icon"
                        color="primary"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="data.icon == 'owners'"
                        icon="users"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="data.icon == 'table'"
                        icon="table"
                        class="prefix-icon"
                        color="primary"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="data.icon == 'schema'"
                        icon="table-columns"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="data.icon == 'unlock'"
                        icon="unlock"
                        class="prefix-icon"
                        color="primary"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="data.icon == 'lock'"
                        icon="lock"
                        class="prefix-icon"
                        color="primary"
                    ></font-awesome-icon>
                    <span>
                      {{ $t(node.label) }}
                      <el-tag
                          v-if="data.type"
                          class="ml-2 info-tag"
                          type="info"
                          effect="plain"
                          round
                      >
                        {{ data.type }}
                      </el-tag>
                      <el-tag
                          v-if="data.type"
                          class="ml-2 info-tag"
                          effect="plain"
                          round
                      >
                        {{ data.modifier }}
                      </el-tag>
                    </span>
                  </div>
                </span>
              </template>
            </el-tree>
          </el-collapse-item>
        </el-collapse>
        <el-collapse class="collapse-panel" v-if="hasOwner">
          <el-collapse-item>
            <template #title>
              <div class="collapse-title">
                <font-awesome-icon icon="house" class="prefix-icon" />
                <span>{{ $t("editor.myTables") }}</span>
              </div>
            </template>
            <div v-if="connectState != 2" class="collapse-placeholder">
              <span> {{ $t("editor.disConnected") }} </span>
            </div>
            <el-tree
                v-else
                :data="localTableTree"
                node-key="id"
                class="structure-tree"
            >
              <template #default="{ node, data }">
                <span class="tree-entry">
                  <span>
                    <font-awesome-icon
                        v-if="data.icon == 'published'"
                        icon="upload"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="data.icon == 'local'"
                        icon="database"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="node.level == 2"
                        icon="table"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <font-awesome-icon
                        v-if="node.level == 3"
                        icon="table-columns"
                        class="prefix-icon"
                    ></font-awesome-icon>
                    <span v-if="node.level == 1"> {{ $t(data.label) }} </span>
                    <span v-if="node.level == 2"> {{ data.label }} </span>
                    <span v-if="node.level == 3">
                      {{ data.label }}
                      <el-tag
                          class="mx-1 info-tag"
                          type="info"
                          effect="plain"
                          round
                      >
                        {{ data.type }}
                      </el-tag>
                      <el-tag
                          v-if="data.modifier"
                          class="mx-1 info-tag"
                          type="primary"
                          effect="plain"
                          round
                      >
                        {{ data.modifier }}
                      </el-tag>
                      <el-tag
                          @click="changeDesensitization(data, node)"
                          v-if="data.desensitize && desensitizeFlag"
                          class="mx-1 info-tag"
                          type="primary"
                          effect="plain"
                          round
                      >
                        {{ data.desensitize.sensitivity }}
                      </el-tag>
                      <el-tag
                          @click="changeDesensitization(data, node)"
                          v-if="data.desensitize && data.desensitize.sensitivity == 'SENSITIVE' && desensitizeFlag"
                          class="mx-1 info-tag"
                          type="success"
                          effect="plain"
                          round
                      >
                        {{ data.desensitize.method.type}}
                      </el-tag>
                    </span>
                  </span>
                  <font-awesome-icon
                      v-if="data.removable"
                      icon="trash-can"
                      @click="removePublishedSchema(data.label)"
                  ></font-awesome-icon>
                </span>
              </template>
            </el-tree>
          </el-collapse-item>
        </el-collapse>
      </el-scrollbar>
    </pane>
    <pane class="right-pane">
      <el-header class="top-bar">
        <el-menu
            mode="horizontal"
            @select="changeTab"
            default-active="1"
            background-color="#ececec"
            text-color="#666"
            active-text-color="#409eff"
        >
          <el-menu-item index="1">
            <font-awesome-icon icon="gauge" class="prefix-icon" />
            {{ $t("editor.dashBoard") }}
          </el-menu-item>
          <el-menu-item index="2">
            <font-awesome-icon icon="magnifying-glass" class="prefix-icon" />
            {{ $t("editor.queryPanel") }}
          </el-menu-item>
        </el-menu>
      </el-header>
      <el-main v-if="activeTab == 1" class="main-pane">
        <el-row class="vertical-split" justify="space-around">
          <el-col v-if="connectState == 2" :span="16">
            <div class="horizontal-split">
              <el-card shadow="hover" class="card-pane" v-if="hasOwner">
                <template #header>
                  <div class="card-header">
                    <span>{{ $t("editor.publishLocalTableSchema") }}</span>
                  </div>
                </template>
                <div class="card-content">
                  <div v-if="editPublishedSchema">
                    <el-input
                        class="table-selector"
                        v-model="publishedTableName"
                        :placeholder="$t('editor.nameOfPublishedSchema')"
                    >
                      <template #prepend>
                        <el-select
                            v-model="selectedLocalTableIndex"
                            :placeholder="$t('editor.selectLocalTable')"
                            @change="selectedLocalTable"
                        >
                          <el-option
                              v-for="(item, index) in localTables"
                              :key="item.name"
                              :label="item.name"
                              :value="index"
                          />
                        </el-select>
                      </template>
                    </el-input>
                    <el-tree
                        v-if="publishedColumns.length > 0"
                        :data="publishedColumns"
                        draggable
                        :allow-drop="allowDrop"
                        @node-drop="handleDrop"
                        class="draggable-tree"
                    >
                      <template #default="{ data }">
                        <div class="tree-entry">
                          <span class="column-name">
                            <el-input
                                v-model="data.name"
                                :placeholder="$t('editor.nameOfPublishedColumn')"
                                class="entry-form"
                            >
                            </el-input>
                          </span>
                          <span class="constraint-selector">
                            Constraint:
                            <el-select
                                v-model="data.modifier"
                                :placeholder="$t('editor.select')"
                                class="entry-form"
                            >
                              <el-option
                                  v-for="item in modifierOptions"
                                  :key="item.value"
                                  :label="item.label"
                                  :value="item.value"
                              />
                            </el-select>
                          </span>
                        </div>
                      </template>
                    </el-tree>
                    <el-button
                        v-if="publishedColumns.length == 0"
                        type="primary"
                        class="full-width-button bottom-button"
                        disabled
                    >{{ $t("editor.publishTable") }}</el-button
                    >
                    <el-button
                        v-else
                        type="primary"
                        class="full-width-button bottom-button"
                        @click="publishSchema()"
                    >{{ $t("editor.publishTable") }}</el-button
                    >
                  </div>
                  <div class="card-content" v-else>
                    <span>
                      {{ $t("editor.cardContent1") }}
                    </span>
                    <el-button
                        class="full-width-button bottom-button"
                        type="primary"
                        @click="editPublishSchema()"
                    >
                      <font-awesome-icon
                          icon="circle-plus"
                          class="prefix-icon"
                      />
                      {{ $t("editor.try") }}
                    </el-button>
                  </div>
                </div>
              </el-card>
              <el-card shadow="hover" class="card-pane">
                <template #header>
                  <div class="card-header">
                    <span>{{ $t("editor.assembleGlobalSchema") }}</span>
                  </div>
                </template>
                <div class="card-content">
                  <div v-if="assembleGlobalTable">
                    <el-input
                        class="table-selector"
                        v-model="newGlobalTable.tableName"
                        :placeholder="$t('editor.nameOfGlobalTable')"
                    >
                    </el-input>
                    <el-cascader class="component-selector"
                    :placeholder="$t('editor.selectComponentTables')"
                                 :options="ownerTables" :props="{ multiple: true }" clearable v-model="componentTables">
                    </el-cascader>
                    <el-button
                        v-if="newGlobalTable.tableName == '' && componentTables.length > 0"
                        type="primary"
                        class="full-width-button bottom-button"
                        disabled
                    >{{ $t("editor.createGlobalSchema") }}</el-button
                    >
                    <el-button
                        v-else
                        type="primary"
                        class="full-width-button bottom-button"
                        @click="addGlobalTable()"
                    >{{ $t("editor.createGlobalSchema") }}</el-button
                    >
                  </div>
                  <div v-else>
                    <span>
                      {{ $t("editor.cardContent2") }}
                    </span>
                    <el-button
                        class="full-width-button bottom-button"
                        type="primary"
                        @click="editGlobalTable()"
                    >
                      <font-awesome-icon
                          icon="circle-plus"
                          class="prefix-icon"
                      />
                      {{ $t("editor.try") }}
                    </el-button>
                  </div>
                </div>
              </el-card>
            </div>
          </el-col>
        </el-row>
      </el-main>
      <el-main v-if="activeTab == 2" class="main-pane">
        <splitpanes horizontal>
          <pane>
            <editor-sub
                class="sql-editor"
                v-model:value="inputSQL"
            ></editor-sub>
          </pane>
          <pane>
            <div class="tool-bar">
              <el-button type="primary" @click="query()" class="tool-button">
                <font-awesome-icon icon="circle-play" class="prefix-icon" />
                Run
              </el-button>
              <span style="margin-left: 4%">
                Desensitize:
              </span>
              <el-switch v-model="desensitizeFlag" active-text="Open" inactive-text="Close" @click="updateDesensitizationFlag" />
            </div>
            <el-empty v-if="resultTable.length == 0" description="No Result" />
            <el-table
                v-else
                stripe
                height="100%"
                :data="resultTable"
                class="result-table"
            >
              <el-table-column
                  v-for="(col, index) in resultSet.schema.columns"
                  :key="index"
                  :prop="col.name"
                  :label="col.name"
              ></el-table-column>
            </el-table>
          </pane>
        </splitpanes>
      </el-main>
    </pane>
  </splitpanes>
</template>

<script>
import { Splitpanes, Pane } from "splitpanes";
import "splitpanes/dist/splitpanes.css";
import axios from "axios";
import { library } from "@fortawesome/fontawesome-svg-core";
import EditorSub from "@/pages/editor/editor-sub.vue";
import { ElMessageBox } from 'element-plus';
import {ref} from "vue";
import {
  faTable,
  faTableColumns,
  faTableList,
  faGauge,
  faSitemap,
  faMagnifyingGlass,
  faDatabase,
  faServer,
  faXmark,
  faCircleXmark,
  faTrashCan,
  faLayerGroup,
  faUsers,
  faArrowsRotate,
  faLink,
  faCirclePlay,
  faCircleExclamation,
  faSpinner,
  faLock,
  faUnlock,
  faCirclePlus,
  faPlus,
  faUser,
  faUpload,
  faHouse,
} from "@fortawesome/free-solid-svg-icons";
library.add(faTable);
library.add(faTableColumns);
library.add(faTableList);
library.add(faGauge);
library.add(faSitemap);
library.add(faMagnifyingGlass);
library.add(faDatabase);
library.add(faServer);
library.add(faXmark);
library.add(faCircleXmark);
library.add(faTrashCan);
library.add(faLayerGroup);
library.add(faUsers);
library.add(faArrowsRotate);
library.add(faLink);
library.add(faCirclePlay);
library.add(faCircleExclamation);
library.add(faSpinner);
library.add(faLock);
library.add(faUnlock);
library.add(faCirclePlus);
library.add(faPlus);
library.add(faUser);
library.add(faUpload);
library.add(faHouse);

export default {
  components: {
    Splitpanes,
    Pane,
    EditorSub,
  },
  mounted() {
    this.refreshGlobalTableTree();
    this.refreshLocalTableTree();
    this.connectRemote();
    this.updateDesensitizationFlag()
  },
  data() {
    const dialogVisible = ref(false)
    const handleClose = (done) => {
      ElMessageBox.confirm(this.$t('editor.confirmDesensitization'), {
        beforeClose: (action, instance, done) =>  {
          if (action === 'confirm') {
            this.updateDesensitization();
            done()
          } else {
            this.dialogVisible = ref(false);
            done()
          }
        }
      })
          .then(() => {
            done()
          })
          .catch(() => {
            // catch error
          })
    }
    return {
      connectState: 0, // 0: no_connection, 1: connecting, 2: connected, 3: connect_fail
      hasOwner: false,
      hasLocalData: false,
      ownerAddress: "",
      activeTab: 1,
      editPublishedSchema: false,
      publishedTableName: "",
      publishedColumns: [],
      inputSQL: "SELECT * FROM student1",
      owners: [],
      ownerTables: [],
      ownerTree: [],
      localTables: {},
      localTableTree: [
        {
          id: 0,
          label: 'editor.localTable',
          icon: "local",
          children: [],
        },
        {
          id: 1,
          label: 'editor.publishedTable',
          icon: "published",
          children: [],
        },
      ],
      globalTables: [],
      globalTableTree: [],
      assembleGlobalTable: false,
      newGlobalTable: {
        tableName: "",
        localTables: [],
      },
      componentTables: [],
      resultTable: [],
      resultSet: {
        schema: {
          columns: [],
        },
        rows: [],
      },
      selectedLocalTableIndex: null,
      modifierOptions: [
        {
          value: "HIDDEN",
          label: "HIDDEN",
        },
        {
          value: "PUBLIC",
          label: "PUBLIC",
        },
        {
          value: "PROTECTED",
          label: "PROTECTED",
        },
        {
          value: "PRIVATE",
          label: "PRIVATE",
        },
      ],
      sensitivityOptions: [
        {
          value: "PLAIN",
          label: this.$t('editor.sensitivity.plain'),
        },
        {
          value: "SENSITIVE",
          label: this.$t('editor.sensitivity.sensitive'),
        },
        {
          value: "SECRET",
          label: this.$t('editor.sensitivity.secret'),
        }
      ],
      methodOptions: {
        "INT" : [
          {
            value: "MAINTAIN",
            label: this.$t('editor.maintain.type'),
          },
          {
            value: "NUMBER_FLOOR",
            label: this.$t('editor.number_floor.type'),
          }
        ],
        "STRING": [
          {
            value: "MAINTAIN",
            label: this.$t('editor.maintain.type'),
          },
          {
            value: "MASK",
            label: this.$t('editor.mask.type'),
          },
          {
            value: "REPLACE",
            label: this.$t('editor.replace.type'),
          }
        ],
        "DATE": [
          {
            value: "MAINTAIN",
            label: this.$t('editor.maintain.type'),
          },
          {
            value: "DATE_FLOOR",
            label: this.$t('editor.date_floor.type'),
          }
        ],
        "TIME": [
          {
            value: "MAINTAIN",
            label: this.$t('editor.maintain.type'),
          },
          {
            value: "DATE_FLOOR",
            label: this.$t('editor.date_floor.type'),
          }
        ],
        "TIMESTAMP": [
          {
            value: "MAINTAIN",
            label: this.$t('editor.maintain.type'),
          },
          {
            value: "DATE_FLOOR",
            label: this.$t('editor.date_floor.type'),
          }
        ],
      },
      dateFloorOptions: {
        "DATE": [
          {
            value: "year",
            label: this.$t('editor.time.year'),
          },
          {
            value: "month",
            label: this.$t('editor.time.month'),
          }
        ],
        "TIME": [
          {
            value: "hour",
            label: this.$t('editor.time.hour'),
          },
          {
            value: "minute",
            label: this.$t('editor.time.minute'),
          }
        ],
        "TIMESTAMP": [
          {
            value: "year",
            label: this.$t('editor.time.year'),
          },
          {
            value: "month",
            label: this.$t('editor.time.month'),
          },
          {
            value: "day",
            label: this.$t('editor.time.day'),
          },
          {
            value: "hour",
            label: this.$t('editor.time.hour'),
          },
          {
            value: "minute",
            label: this.$t('editor.time.minute'),
          },
          {
            value: "second",
            label: this.$t('editor.time.second'),
          },
        ]
      },
      maintain: {
        type: ""
      },
      mask: {
        type: "",
        begin: 0,
        end: 0,
        str: ""
      },
      replace: {
        type: "",
        fromStr: "",
        toStr: ""
      },
      date_floor: {
        type: "",
        floor: ""
      },
      number_floor: {
        type: "",
        place: 0
      },
      dialogVisible,
      handleClose,
      tableName: "",
      column: {
        name: "",
        type: "",
        modifier: "",
        desensitize: {
          sensitivity: "",
          method: {
            type: ""
          }
        }
      },
      desensitizeFlag: ref(false),
    };
  },
  methods: {
    updateDesensitizationFlag() {
      axios
          .post('/owner/updateDesensitizeFlag', {desensitizeFlag: this.desensitizeFlag})
          .then((response) => {
          })
          .catch(() => {
            window.alert(`Fail to update desensitize flag`);
          })
    },
    updateDesensitization() {
      let type = this.column.desensitize.method.type;
      switch (type) {
        case "MAINTAIN":
          this.maintain.type = type;
          this.column.desensitize.method = this.maintain;
          break;
        case "MASK":
          this.mask.type = type;
          this.mask.begin = parseInt(this.mask.begin);
          this.mask.end = parseInt(this.mask.end);
          this.column.desensitize.method = this.mask;
          break;
        case "REPLACE":
          this.replace.type = type;
          this.column.desensitize.method = this.replace;
          break;
        case "NUMBER_FLOOR":
          this.number_floor.type = type;
          this.number_floor.place = parseInt(this.number_floor.place);
          this.column.desensitize.method = this.number_floor;
          break;
        case "DATE_FLOOR":
          this.date_floor.type = type;
          this.column.desensitize.method = this.date_floor;
          break;
      }
      if (this.column.desensitize.sensitivity == "PLAIN" || this.column.desensitize.sensitivity == "SECRET") {
        this.maintain.type = "MAINTAIN";
        this.column.desensitize.method = this.maintain;
      }
      console.log(this.column)
      axios
          .post('/owner/updateDesensitize', {tableName: this.tableName, columnDesc: this.column})
          .then(() => {
            this.dialogVisible = ref(false);
            this.tableName = "";
            this.column.name = "";
            this.column.type = "";
            this.column.modifier = "";
            this.column.desensitize.sensitivity = "";
            this.column.desensitize.method = {};
            this.refreshLocalTableTree();
          })
          .catch(() => {
            window.alert(`Fail to update desensitize: ${this.column.name} in ${this.tableName}`);
          })
    },
    changeDesensitization(data, node) {
      console.log(data)
      this.dialogVisible = ref(true);
      this.column.name = data.label;
      this.column.type = data.type;
      this.column.modifier = "HIDDEN";
      this.column.desensitize.sensitivity = data.desensitize.sensitivity;
      this.column.desensitize.method.type = data.desensitize.method.type;
      switch (data.desensitize.method.type) {
        case "MAINTAIN":
          //this.column.desensitize.method = this.maintain;
          break;
        case "MASK":
          this.mask.begin = data.desensitize.method.begin;
          this.mask.end = data.desensitize.method.end;
          this.mask.str = data.desensitize.method.str;
          //this.column.desensitize.method = this.mask;
          break;
        case "REPLACE":
          this.replace.fromStr = data.desensitize.method.fromStr;
          this.replace.toStr = data.desensitize.method.toStr;
          //this.column.desensitize.method = this.replace;
          break;
        case "NUMBER_FLOOR":
          this.number_floor.place = data.desensitize.method.place;
          //this.column.desensitize.method = this.number_floor;
          break;
        case "DATE_FLOOR":
          this.date_floor.floor = data.desensitize.method.floor;
          //this.column.desensitize.method = this.date_floor;
          break;
      }
      console.info(this.column);
      this.tableName = node.parent.data.label;
    },
    connectRemote() {
      this.connectState = 1;
      axios
          .get("/alive")
          .then((response) => {
            this.hasOwner = response.data;
            this.connectState = 2;
          })
          .catch((err) => {
            this.connectState = 3;
            console.log(err);
          });
      this.refreshOwnerTree();
      this.refreshLocalTableTree();
      this.refreshGlobalTableTree();
    },
    refreshAll() {
      this.refreshOwnerTree();
      this.refreshLocalTableTree();
      this.refreshGlobalTableTree();
    },
    refreshOwnerTree() {
      axios
          .get("/user/owners")
          .then((response) => {
            this.owners = response.data;
            this.ownerTree = [];
            this.ownerTables = [];
            this.owners.forEach((owner) => {
              this.ownerTree.push({
                id: owner,
                label: owner,
                children: [],
              });
              this.ownerTables.push({
                value: owner,
                label: owner,
                children: [],
              })
              this.getLocalTables(owner, this.ownerTree.length - 1);
            });
          })
          .catch((err) => {
            console.log(err);
          });
    },
    getLocalTables(owner, id) {
      axios
          .get("/user/owners/" + owner)
          .then((response) => {
            this.localTables[owner] = response.data;
            response.data.forEach((table) => {
              const tableNode = {
                id: owner + table.name,
                label: table.name,
                children: [],
              };
              table.schema.columns.forEach((column) => {
                tableNode.children.push({
                  id: owner + table.name + column.name,
                  name: column.name,
                  type: column.type,
                  modifier: column.modifier,
                });
              });
              this.ownerTree[id].children.push(tableNode);
              this.ownerTables[id].children.push({
                label: table.name,
                value: table.name,
              });
            });
          })
          .catch((err) => {
            console.log(err);
          });
    },
    refreshGlobalTableTree() {
      axios
          .get("/user/globaltables")
          .then((response) => {
            this.globalTables = response.data;
            this.globalTableTree = [];
            this.globalTables.forEach((gTable) => {
              // todo: add schema as a child node
              const gTableNode = {
                id: gTable.name,
                label: gTable.name,
                children: [
                  {
                    id: gTable.name + "$schema",
                    label: "editor.schema",
                    icon: "schema",
                    children: gTable.schema.columns.map((column) => {
                      return {
                        id: gTable.name + "#" + column.name,
                        label: column.name,
                        icon: column.modifier == "PUBLIC" ? "unlock" : "lock",
                        type: column.type,
                        modifier: column.modifier,
                        children: [],
                      };
                    }),
                  },
                  {
                    id: gTable.name + "$owners",
                    label: "editor.owners",
                    icon: "owners",
                    children: gTable.mappings.map((meta) => {
                      return {
                        id: gTable.name + meta.endpoint,
                        label: meta.endpoint,
                        icon: "owner",
                        children: [
                          {
                            id: gTable.name + meta.endpoint + meta.localName,
                            label: meta.localName,
                            icon: "table",
                            children: [],
                          },
                        ],
                      };
                    }),
                  },
                ],
              };
              this.globalTableTree.push(gTableNode);
            });
          })
          .catch((err) => {
            console.log(err);
          });
    },
    refreshLocalTableTree() {
      axios
          .get("/owner/localtables")
          .then((response) => {
            this.localTables = response.data;
            this.localTableTree[0].children = [];
            this.localTables.forEach((table) => {
              const treeNode = {
                id: table.name,
                label: table.name,
                children: [],
              };
              table.schema.columns.forEach((column) => {
                treeNode.children.push({
                  id: table.name + column.name,
                  label: column.name,
                  type: column.type,
                  desensitize: column.desensitize,
                  children: [],
                });
              });
              this.localTableTree[0].children.push(treeNode);
            });
          })
          .catch((err) => {
            console.log(err);
          });
      axios
          .get("/owner/publishedtables")
          .then((response) => {
            this.publishedTables = response.data;
            this.localTableTree[1].children = [];
            this.publishedTables.forEach((table) => {
              const treeNode = {
                id: table.publishedName,
                label: table.publishedName,
                removable: true,
                children: [],
              };
              table.publishedColumns.forEach((column) => {
                treeNode.children.push({
                  id: table.publishedColumns + column.name,
                  label: column.name,
                  type: column.type,
                  modifier: column.modifier,
                  children: [],
                });
              });
              this.localTableTree[1].children.push(treeNode);
            });
          })
          .catch((err) => {
            console.log(err);
          });
    },
    addOwner() {
      axios
          .post("/user/owners", { value: this.ownerAddress })
          .then(() => {
            this.refreshOwnerTree();
            this.ownerAddress = "";
          })
          .catch(() => {
            window.alert(`Fail to connect ${this.ownerAddress}`);
          });
    },
    removeOwner(endpoint) {
      axios
          .delete("/user/owners/" + endpoint)
          .then(() => {
            this.refreshOwnerTree();
            this.refreshGlobalTableTree();
          })
          .catch((err) => {
            console.log(err);
            window.alert(`Fail to connect ${endpoint}`);
          });
    },
    query() {
      let sql = this.inputSQL.trim();
      if (sql.charAt(sql.length - 1) == ";") {
        sql = sql.substring(0, sql.length - 1);
      }
      axios
          .post("/user/query", { value: sql })
          .then((response) => {
            this.resultTable = this.parseResult(response.data);
            this.resultSet = response.data;
          })
          .catch((err) => {
            console.log(err);
            window.alert(`Fail to query ${this.inputSQL}`);
          });
    },
    parseResult(resultSet) {
      const columns = resultSet.schema.columns;
      const rows = resultSet.rows;
      let tableData = [];
      for (const j in rows) {
        let r = {};
        for (const i in columns) {
          r[columns[i].name] = rows[j][i];
        }
        tableData.push(r);
      }
      return tableData;
    },
    editPublishSchema() {
      this.editPublishedSchema = true;
    },
    selectedLocalTable(tableIndex) {
      this.publishedColumns = this.localTables[tableIndex].schema.columns.map(
          (column, index) => {
            return {
              id: index,
              label: column.name,
              type: column.type,
              name: column.name,
              modifier: "HIDDEN",
            };
          }
      );
      this.publishedTableName = this.localTables[tableIndex].name;
    },
    publishSchema() {
      let columns = this.publishedColumns.map((column) => {
        return {
          name: column.name,
          type: column.type,
          modifier: column.modifier,
          columnId: column.id
        };
      });
      console.log(columns);
      const publishedSchema = {
        publishedName:
            this.publishedTableName == ""
                ? this.localTables[this.selectedLocalTableIndex].name
                : this.publishedTableName,
        actualName: this.localTables[this.selectedLocalTableIndex].name,
        publishedColumns: columns
      };
      axios
          .post("/owner/publishedtables", publishedSchema)
          .then(() => {
            this.refreshOwnerTree();
            this.refreshLocalTableTree();
            this.ownerAddress = "";
            this.editPublishedSchema = false;
            this.publishedTableName = "";
            this.selectedLocalTableIndex = null;
            this.publishedColumns = [];
          })
          .catch(() => {
            window.alert(
                `Fail to publish schema ${this.publishedSchema.publishedName}`
            );
          });
      console.log(publishedSchema);
    },
    removePublishedSchema(publishedName) {
      axios
          .delete("/owner/publishedtables/" + publishedName)
          .then(() => {
            this.refreshLocalTableTree();
            this.refreshOwnerTree();
          })
          .catch((err) => {
            console.log(err);
            window.alert(`Fail to remove published schema ${publishedName}`);
          });
    },
    allowDrop(_, __, type) {
      return type != "inner";
    },
    editGlobalTable() {
      this.assembleGlobalTable = true;
    },
    checkGlobalTableName() {
      if (this.newGlobalTable.tableName == "") {
        return "Global table name con not be empty";
      }
      for (let i = 0; i < this.globalTables.length; ++i) {
        if (this.newGlobalTable.tableName == this.globalTables[i].name) {
          return `Global table ${this.newGlobalTable.tableName} already existed`;
        }
      }
      return true;
    },
    checkGlobalTable() {
      if (this.newGlobalTable.tableName == "") {
        window.alert("Global Table name can not be empty");
        return false;
      }
      this.newGlobalTable.localTables = []
      for (let i = 0; i < this.componentTables.length; ++i) {
        this.newGlobalTable.localTables.push({
          endpoint: this.componentTables[i][0],
          localName: this.componentTables[i][1],
        })
      }
      return true;
    },
    addGlobalTable() {
      if (!this.checkGlobalTable()) {
        return;
      }
      axios
          .post("/user/globaltables", this.newGlobalTable)
          .then(() => {
            this.refreshGlobalTableTree();
            this.newGlobalTable.tableName = "";
            this.newGlobalTable.localTables = []
            this.assembleGlobalTable = false;
          })
          .catch((err) => {
            console.log(err);
            window.alert(
                `Fail to add global table ${this.newGlobalTable.tableName}`
            );
          });
    },
    delGlobalTable(name) {
      axios.delete("/user/globaltables/" + name).then(() => {
        this.refreshGlobalTableTree();
      });
    },
    jump2DashBoard() {
      this.activeTab = 1;
    },
    changeTab(index) {
      this.activeTab = index;
    },
  },
};
</script>

<style lang="css">
.app-container {
  height: 100%;
}
.app-container .corner-button {
  float: right;
}
.left-pane {
  overflow: hidden;
  overflow-wrap: anywhere;
  background-color: #f8f8f8;
}
.right-pane {
  overflow: hidden;
}
.right-pane .pane-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-direction: column;
  overflow: hidden;
  width: 100%;
  padding: 15px;
}
.right-pane .main-pane {
  height: 100%;
  padding: 0px;
}
.card-pane {
  display: flex;
  max-width: 800px;
  width: 80%;
  margin: 20px;
  background-color: #ececec;
  align-content: space-evenly;
  flex-direction: column;
}
.card-header {
  font-size: 20px;
  font-weight: bold;
  line-height: 32px;
  height: 32px;
  display: flex;
  justify-content: space-between;
  align-content: center;
  overflow: hidden;
}
.card-content {
  font-size: 16px;
  line-height: 24px;
  text-align: left;
}

.app-container .top-bar {
  height: 30px;
  font-size: 16px;
  line-height: 30px;
  padding: 0;
  overflow: hidden;
}
.connect-info {
  padding: 0 10px !important;
  display: flex;
  align-items: flex-start;
  align-content: center;
  justify-content: space-between;
}
.left-pane .el-collapse-item__header {
  padding-left: 10px;
  height: 32px;
  line-height: 32px;
  font-size: 16px;
  overflow: hidden;
}
.left-pane .collapse-panel .el-collapse-item__content {
  padding-bottom: 0;
  border-top: 1px solid #d9ecff;
}
.left-pane .structure-tree .el-tree-node__content {
  height: 32px;
  font-size: 16px;
}
.structure-tree .info-tag {
  margin-left: 5px;
}
.structure-tree .tree-entry {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding-right: 10px;
}
.right-pane .el-menu {
  height: 100%;
  font-size: 18px;
  line-height: 30px;
  width: 100%;
}
.right-pane .vertical-split {
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: space-around;
  flex-wrap: nowrap;
}
.right-pane .horizontal-split {
  display: flex;
  justify-content: center;
  width: 100%;
  flex-direction: column;
  align-items: center;
}
.collapse-panel .collapse-placeholder {
  font-size: 16px;
  color: #666;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-direction: column;
  padding: 5px 0;
}
.collapse-panel .collapse-content {
  width: 100%;
}
.collapse-panel .collapse-title {
  width: 200px;
  display: flex;
  justify-content: left;
  height: 32px;
  line-height: 32px;
  align-items: center;
}
.tool-bar {
  display: flex;
  justify-content: left;
  align-items: center;
  height: 50px;
  width: 100%;
  background-color: white;
  padding: 5px 10px;
}

.tool-bar .tool-button {
  width: 100px;
}
.collapse-panel .collapse-title span {
  height: 32px;
  line-height: 32px;
}
.horizontal-tabs {
  min-width: 150px;
  max-width: 350px;
  width: 100%;
}
.main-pane .line-input {
  width: 100%;
  padding-bottom: 10px;
}
.el-menu--horizontal {
  justify-content: space-around;
}
.horizontal-tabs .el-tabs__header {
  margin-bottom: 10px;
}
.prefix-icon {
  margin: 0 8px 0 0;
}
.left-pane .group-form {
  padding: 5px 10px;
}
.group-form .form-line {
  padding-bottom: 5px;
}
.hufu-name {
  font-size: 30px;
}
.full-width-button {
  width: 100%;
}
.bottom-button {
  margin-top: 10px;
}
.draggable-tree .el-tree-node__content {
  height: 32px;
  font-size: 15px;
  line-height: 32px;
  background-color: #f5f7fa;
  border-radius: 4px;
  border: 1px solid #e4e7ed;
  overflow: hidden;
}
.draggable-tree .tree-entry {
  display: flex;
  justify-content: space-between;
  width: 100%;
  align-items: center;
}
.draggable-tree .el-tree-node__content .el-tree-node__expand-icon {
  padding: 0;
  width: 0;
  height: 0;
}
.draggable-tree .tree-entry .entry-form {
  background-color: transparent;
  border: 0;
  box-shadow: none;
}
.draggable-tree .tree-entry .entry-form .el-input__wrapper {
  background-color: transparent;
  border: 0;
  box-shadow: none;
}
.sql-editor {
  padding: 0;
  width: 100%;
  height: 100%;
  text-align: left;
}
.icon-button {
  cursor: pointer;
}
.column-name {
  font-weight: bold;
  padding-right: 20px;
}
.constraint-selector {
  font-size: 15px;
}
.constraint-selector .el-input {
  width: 100px;
}
.result-table {
  padding: 0 10px;
}
.splitpanes__splitter {
  background-color: #d9ecff;
  position: relative;
}
.splitpanes__splitter:before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  transition: opacity 0.4s;
  background-color: rgba(217, 236, 255, 0.5);
  opacity: 0;
  z-index: 1;
}
.splitpanes__splitter:hover:before {
  opacity: 1;
}
.splitpanes--vertical > .splitpanes__splitter:before {
  left: -2px;
  right: -10px;
  height: 100%;
}
.splitpanes--horizontal > .splitpanes__splitter:before {
  top: -5px;
  bottom: -5px;
  width: 100%;
}
.component-selector {
  width: 100%;
  padding: 10px 0;
}
@keyframes spin {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}
.rotate-element {
  animation: spin 2s infinite;
}
.dialog-footer button:first-child {
  margin-right: 10px;
}
</style>
