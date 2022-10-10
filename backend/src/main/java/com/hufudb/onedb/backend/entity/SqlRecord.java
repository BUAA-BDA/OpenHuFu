package com.hufudb.onedb.backend.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import com.baomidou.mybatisplus.annotation.TableId;
import java.time.LocalDateTime;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * <p>
 * 
 * </p>
 *
 * @author qlh
 * @since 2022-10-21
 */
@Data
@EqualsAndHashCode(callSuper = false)
@TableName("sqlRecord")
public class SqlRecord extends Model<SqlRecord> {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private String context;

    private String userName;

    private String status;

    private LocalDateTime startTime;

    private LocalDateTime endTime;


    @Override
    protected Serializable pkVal() {
        return this.id;
    }

}
