package com.hufudb.onedb.backend.utils;

import com.github.pagehelper.PageHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class PageUtils {

  public static <E> Page<E> getPage(Supplier<List<E>> method, int page, int size) {
    com.github.pagehelper.Page<E> originPage = PageHelper.startPage(page, size);
    return getPage(method, originPage, page, size);
  }

  public static <E> Page<E> getPage(Supplier<List<E>> method, int page, int size, String order) {
    com.github.pagehelper.Page<E> originPage = PageHelper.startPage(page, size, order);
    return getPage(method, originPage, page, size);
  }


  public static <E> Page<E> getPage(Supplier<List<E>> method,
      com.github.pagehelper.Page<E> originPage, int page, int size) {
    List<E> result = method.get();
    Page<E> resultPage = new Page<>();
    resultPage.setData(result);

    Pagination pagination = new Pagination();
    pagination.setTotal((int) originPage.getTotal());
    if (0 == originPage.getPageSize()) {
      pagination.setPage(page);
      pagination.setSize(size);
    } else {
      pagination.setPage(originPage.getPageNum());
      pagination.setSize(originPage.getPageSize());
    }
    resultPage.setPagination(pagination);
    return resultPage;
  }

  public static <E> Page<E> getPage(List<E> pagingList, int pageNum, int pageSize) {
    if (pagingList == null) {
      throw new IllegalArgumentException("Paging list can not be null");
    }

    Page<E> page = new Page<>();
    if (pageNum <= 0 || pageSize <= 0) {
      Pagination pagination = new Pagination(pagingList.size(),0,0);
      page.setPagination(pagination);
      page.setData(pagingList);
    } else {
      Pagination pagination = new Pagination(pagingList.size(),pageNum,pageSize);
      page.setPagination(pagination);
      page.setData(subList(pagingList, pageNum, pageSize));
    }

    return page;
  }

  private static <E> List<E> subList(List<E> list, int pageNum, int pageSize) {
    int size = list.size();
    List<E> result = new ArrayList<>();
    int idx = (pageNum - 1) * pageSize;
    int end = idx + pageSize;
    while (idx < size && idx < end) {
      result.add(list.get(idx));
      idx++;
    }
    return result;
  }
}
