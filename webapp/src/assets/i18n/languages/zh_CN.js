module.exports =  {
    language: {
        locale: "语言",
    },
    menu: {
        overview: "概览",
        editor: "SQL编辑器",
        operation: "操作",
        datasource: "数据源",
        setting: "设置"
    },
    editor: {
        disConnected: "未连接",
        connected: "已连接",
        failToConnectBackend: "连接失败",
        owners: "数据拥有方",
        noOwner: "无数据拥有方",
        globalTables: "全局表",
        newOwner: "添加新的数据拥有方",
        noGlobalTable: "无全局表",
        myTables: "我的表",
        dashBoard: "控制面板",
        queryPanel: "查询",
        publishLocalTableSchema: "发布本地表",
        publishTable: "发布",
        cardContent1: "发布本地表，允许用户在保证安全性的前提下使用本地表进行联邦查询。",
        try: "尝试",
        assembleGlobalSchema: "汇集全局表",
        cardContent2: "集合多个数据拥有方已发布的表以创建全局表",
        createGlobalSchema: "创建全局表",
        nameOfPublishedSchema: "发布表的名字",
        selectLocalTable: "选择本地表",
        nameOfPublishedColumn: "发布列的名字",
        select: "选择",
        nameOfGlobalTable: "全局表的名字",
        selectComponentTables: "选择组件表",
        localTable: "本地表",
        publishedTable: "已发布的表",
        schema: "模式",
        owners: "数据拥有方",
        confirmDesensitization: "确定修改此列的脱敏方法么？",
        tableName: "表名",
        colName: "列名",
        colType: "列类型",
        colSensitivity: "列敏感度",
        methodType: "脱敏方法",
        sensitivity: {
            plain: "明文",
            sensitive: "敏感",
            secret: "绝密"
        },
        maintain: {
            type: "保持"
        },
        mask: {
            type: "掩码",
            begin: "起始",
            end: "终止",
            str: "掩码字符"
        },
        replace: {
            type: "替换",
            fromStr: "源字符串",
            toStr: "目的字符串"
        },
        number_floor: {
            type: "整数模糊低位",
            place: "模糊位数"
        },
        date_floor: {
            type: "时间模糊低位",
            floor: "模糊单位"
        },
        time: {
            year: "年",
            month: "月",
            day: "日",
            hour: "时",
            minute: "分",
            second: "秒"
        }
    },
    operation: {
        pleaseInput: "请输入",
        pleaseSelect: "请选择",
        query: "查询",
        reset: "重置",
        submitted: "已提交",
        inProgress: "正在执行",
        succeed: "执行成功",
        failed: "执行失败",
        warning: "警告",
        status: "状态",
        context: "查询语句",
        id: "ID",
        username: "用户名",
        subTime: "提交时间",
        execTime: "执行时长",
        noData: "无记录",
        updateDesensitization: "更新并提交"
    },
    setting:{
        id: "ID",
        port: "端口",
        ip: "IP地址",
        status: "状态",
        tables: "已发布表",
        operation: "操作",
        delete: "删除",
        connected: "在线",
        disconnected: "离线",
        address: "地址",
        newOwner: "增加数据拥有方",
        addAddress: "请输入用户IP地址"
    },
    datasource: {
        datasourceType: "数据源类型",
        jdbcUrl: "JDBC连接串",
        username: "用户名",
        password: "密码",
        tables: "数据表",
        testConnection: "测试连接",
        confirm: "确认",
        chooseDatasourceType: "请选择数据源类型",
        chooseTables: "请选择数据表",
    },
}