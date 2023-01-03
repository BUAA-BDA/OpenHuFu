package com.hufudb.onedb.desensitize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public enum ExpSensitivity {
    PLAIN,
    SINGLE_SENSITIVE,
    MULTI_SENSITIVE,

    ERROR;

    public static class ExpressionConvert {
        private static List<Integer> list1 = Arrays.asList(0, 0, 2);
        private static List<Integer> list2 = Arrays.asList(0, 1, 1);
        private static List<Integer> list3 = Arrays.asList(0, 2, 0);
        private static List<Integer> list4 = Arrays.asList(1, 0, 1);
        private static List<Integer> list5 = Arrays.asList(1, 1, 0);
        private static List<Integer> list6 = Arrays.asList(2, 0, 0);

        public static ExpSensitivity convertBinary(Map<ExpSensitivity, Integer> map) {
            Integer plain = map.get(PLAIN) == null ? 0 : map.get(PLAIN);
            Integer singleSensitive = map.get(SINGLE_SENSITIVE) == null ? 0 : map.get(SINGLE_SENSITIVE);
            Integer multiSensitive = map.get(MULTI_SENSITIVE) == null ? 0 : map.get(MULTI_SENSITIVE);
            List<Integer> list = Arrays.asList(plain, singleSensitive, multiSensitive);
            if (list.equals(list1)) {
                return MULTI_SENSITIVE;
            }
            if (list.equals(list2)) {
                return MULTI_SENSITIVE;
            }
            if (list.equals(list3)) {
                return MULTI_SENSITIVE;
            }
            if (list.equals(list4)) {
                return MULTI_SENSITIVE;
            }
            if (list.equals(list5)) {
                return ERROR;
            }
            if (list.equals(list6)) {
                return PLAIN;
            }
            return ERROR;
        }
    }


}
