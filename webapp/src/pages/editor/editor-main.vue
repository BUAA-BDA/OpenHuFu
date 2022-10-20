<template>
  <splitpanes class="app-container" id="app">
    <pane class="left-pane">
      <el-scrollbar>
        <el-header class="top-bar connect-info" v-if="connectState == 0">
          <span>
            <font-awesome-icon icon="circle-exclamation" class="prefix-icon" />
            No Connection
          </span>
        </el-header>
        <el-header class="top-bar connect-info" v-if="connectState == 1">
          <span>
            <font-awesome-icon
                icon="spinner"
                class="prefix-icon rotate-element"
            />
            Connecting to Backend
          </span>
        </el-header>
        <el-header class="top-bar connect-info" v-if="connectState == 2">
          <span>
            <font-awesome-icon
                icon="arrows-rotate"
                class="prefix-icon icon-button"
                @click="refreshAll()"
            />
            Connected to Backend
          </span>
        </el-header>
        <el-header class="top-bar connect-info" v-if="connectState == 3">
          <span>
            <font-awesome-icon icon="circle-exclamation" class="prefix-icon" />
            Fail to Connect Backend
          </span>
        </el-header>
        <el-collapse class="collapse-panel">
          <el-collapse-item>
            <template #title>
              <div class="collapse-title">
                <font-awesome-icon icon="users" class="prefix-icon" />
                <span>Owners</span>
              </div>
            </template>
            <div v-if="connectState != 2" class="collapse-placeholder">
              <span> No connection </span>
            </div>
            <div
                v-if="connectState == 2 && owners.length == 0"
                class="collapse-placeholder"
            >
              <span> No owner </span>
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
                      placeholder="New owner"
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
                <span>Global Tables</span>
              </div>
            </template>
            <div v-if="connectState != 2" class="collapse-placeholder">
              <span> No connection </span>
            </div>
            <div
                v-if="connectState == 2 && globalTables.length == 0"
                class="collapse-placeholder"
            >
              <span> No global table </span>
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
                      {{ node.label }}
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
                <span>My Tables</span>
              </div>
            </template>
            <div v-if="connectState != 2" class="collapse-placeholder">
              <span> No Connection </span>
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
                    <span v-if="node.level == 1"> {{ data.label }} </span>
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
            Dash Board
          </el-menu-item>
          <el-menu-item index="2">
            <font-awesome-icon icon="magnifying-glass" class="prefix-icon" />
            Query Panel
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
                    <span>Publish Local Table Schema</span>
                  </div>
                </template>
                <div class="card-content">
                  <div v-if="editPublishedSchema">
                    <el-input
                        class="table-selector"
                        v-model="publishedTableName"
                        placeholder="Name of Published Schema"
                    >
                      <template #prepend>
                        <el-select
                            v-model="selectedLocalTableIndex"
                            placeholder="Select Local Table"
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
                                placeholder="Name of published Column"
                                class="entry-form"
                            >
                            </el-input>
                          </span>
                          <span class="constraint-selector">
                            Constraint:
                            <el-select
                                v-model="data.modifier"
                                placeholder="Select"
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
                    >Publish Table</el-button
                    >
                    <el-button
                        v-else
                        type="primary"
                        class="full-width-button bottom-button"
                        @click="publishSchema()"
                    >Publish Table</el-button
                    >
                  </div>
                  <div class="card-content" v-else>
                    <span>
                      Publish schemas of local tables, allowing users to perform
                      federated queries over local tables with security
                      constraints.
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
                      Try
                    </el-button>
                  </div>
                </div>
              </el-card>
              <el-card shadow="hover" class="card-pane">
                <template #header>
                  <div class="card-header">
                    <span>Assemble Global Schema</span>
                  </div>
                </template>
                <div class="card-content">
                  <div v-if="assembleGlobalTable">
                    <el-input
                        class="table-selector"
                        v-model="newGlobalTable.tableName"
                        placeholder="Name of Global Table"
                    >
                    </el-input>
                    <el-cascader class="component-selector"
                                 placeholder="Select component tables"
                                 :options="ownerTables" :props="{ multiple: true }" clearable v-model="componentTables">
                    </el-cascader>
                    <el-button
                        v-if="newGlobalTable.tableName == '' && componentTables.length > 0"
                        type="primary"
                        class="full-width-button bottom-button"
                        disabled
                    >Create Global Schema</el-button
                    >
                    <el-button
                        v-else
                        type="primary"
                        class="full-width-button bottom-button"
                        @click="addGlobalTable()"
                    >Create Global Schema</el-button
                    >
                  </div>
                  <div v-else>
                    <span>
                      Assemble published schemas from multiple owners to create
                      a global table schema.
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
                      Try
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
    this.refreshOwnerTree();
    this.refreshGlobalTableTree();
    this.refreshLocalTableTree();
    this.connectRemote();
  },
  data() {
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
          label: "Local table",
          icon: "local",
          children: [],
        },
        {
          id: 1,
          label: "Published table",
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
    };
  },
  methods: {
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
                    label: "schema",
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
                    label: "owners",
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
  margin-bottom: 10;
}
.prefix-icon {
  margin: 0 8px 0 0;
}
.hufu-logo {
  width: 80%;
  padding: 10px;
  max-width: 250px;
  min-width: 150px;
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
</style>
