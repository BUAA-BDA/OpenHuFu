package com.hufudb.onedb.backend.utils;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "page result")

public class Pagination {

  @ApiModelProperty("pageId")
  private int page;

  @ApiModelProperty("total")
  private int total;

  @ApiModelProperty("pageSize")
  private int size;

  public Pagination() {
  }

  public Pagination(int total, int page, int size) {
    this.total = total;
    this.page = page;
    this.size = size;
  }

  public int getPage() {
    return page;
  }

  public void setPage(int page) {
    this.page = page;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }
}