package com.hufudb.onedb.backend.utils;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel(description = "page result")
public class Page<E> {

  @ApiModelProperty("pagination information")
  private Pagination pagination;

  @ApiModelProperty("data")
  private List<E> data;

  public Page() {
  }

  public Pagination getPagination() {
    return pagination;
  }

  public void setPagination(Pagination pagination) {
    this.pagination = pagination;
  }

  public List<E> getData() {
    return data;
  }

  public void setData(List<E> data) {
    this.data = data;
  }
}