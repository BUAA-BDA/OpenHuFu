package com.hufudb.onedb.backend.controller;

import com.hufudb.onedb.backend.entity.SqlRecord;
import com.hufudb.onedb.backend.utils.RecordRequest;

import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@RestController

public class DBController {
    private static final Logger logger = LoggerFactory.getLogger(DBController.class);


    @Resource
    private JdbcTemplate jdbcTemplate;

    private int t = 0;

    public static Connection connection;
    public static long id;

    public DBController(Connection connection) {
        this.connection = connection;
        getMaxId();
    }

    @GetMapping("/test")
    public int test() throws InterruptedException {
        Thread.sleep(1000*5);
        t = t+1;
        return t;
    }

    @PostMapping("/sqlRecord/query")
    public List<SqlRecord> querySqlRecord(@RequestBody RecordRequest request) throws SQLException {
        List<SqlRecord> sqlRecordList = new ArrayList<>();
        String sql = null;
        PreparedStatement ps = null;
        if (request.context == null) {
            if (request.status == null) {
                sql = "SELECT * from sqlRecord";
            } else {
                sql = String.format("SELECT * from sqlRecord where status = '%s'", request.status);
            }
        } else {
            if (request.status == null) {
                sql = String.format("SELECT * from sqlRecord where context like '%%%s%%'", request.context);
            } else {
                sql = String.format("SELECT * from sqlRecord where context like '%%%s%%' and status = '%s'", request.context, request.status);
            }
        }
        System.out.println(sql);
        logger.info(sql);
        ps = connection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            sqlRecordList.add(new SqlRecord(rs.getLong("id"),
                    rs.getString("context"), rs.getString("username"),
                    rs.getString("status"), rs.getTimestamp("subTime"),
                    rs.getTimestamp("startTime"), rs.getLong("execTime")));
        }

        return sqlRecordList;
    }

    public static void getMaxId()  {
        String sql = null;
        PreparedStatement ps = null;
        sql = "SELECT max(id) as max_id from sqlRecord";
        try {
            ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                id = rs.getLong("max_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            id = 0;
        }
    }

}
