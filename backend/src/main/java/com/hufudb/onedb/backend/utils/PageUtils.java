package com.hufudb.onedb.backend.utils;

import com.github.pagehelper.PageHelper;
import java.util.List;
import java.util.function.Supplier;

public class PageUtils {

    public static <E> Page<E> getPage(Supplier<List<E>> method, int page, int size) {
        com.github.pagehelper.Page<E> originPage = PageHelper.startPage(page, size);
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

    public static <E> Page<E> getPage(Supplier<List<E>> method, int page, int size, String order) {
        com.github.pagehelper.Page<E> originPage = PageHelper.startPage(page, size, order);
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
}
