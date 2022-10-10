package com.hufudb.onedb.backend.entity;

import com.google.type.DateTime;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

public class SqlRecord implements Serializable{

    private long id;
    private String context;
    private String username;
    private String status;

    private Timestamp startTime;
    private Timestamp subTime;
    private long execTime;

    public SqlRecord(long id, String context, String username, String status, Timestamp subTime, Timestamp startTime, long execTime) {
        this.id = id;
        this.context = context;
        this.username = username;
        this.status = status;
        this.subTime = subTime;
        this.startTime = startTime;
        this.execTime = execTime;
    }

    public long getId() {
        return id;
    }

    public String getContext() {
        return context;
    }

    public String getUsername() {
        return username;
    }

    public String getStatus() {
        return status;
    }

    public Date getSubTime() {
        return subTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public long getExecTime() {
        return execTime;
    }


    public void setId(long id) {
        this.id = id;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setSubTime(Timestamp subTime) {
        this.subTime = subTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public void setExecTime(long execTime) {
        this.execTime = execTime;
    }

}

