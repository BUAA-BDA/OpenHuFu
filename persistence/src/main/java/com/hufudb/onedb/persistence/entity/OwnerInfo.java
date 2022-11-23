package com.hufudb.onedb.persistence.entity;

import java.io.Serializable;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * <p>
 *
 * </p>
 *
 * @author fzh
 * @since 2022-11-4
 */
@Data
@EqualsAndHashCode(callSuper = false)
@TableName("owner")
public class OwnerInfo extends Model<OwnerInfo>{
    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private String address;

    private String userName;

    private String status;

    private Long tableNum;

    @Override
    protected Serializable pkVal() {
        return this.id;
    }

    public Long getId() {
        return id;
    }

    public String getAddress(){
        return address;
    }
}
