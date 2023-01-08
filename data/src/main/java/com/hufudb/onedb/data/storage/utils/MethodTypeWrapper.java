package com.hufudb.onedb.data.storage.utils;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.hufudb.onedb.proto.OneDBData;

public enum MethodTypeWrapper {

    @SerializedName(value = "MAINTAIN", alternate = "maintain")
    MAINTAIN("MAINTAIN", OneDBData.MethodType.MAINTAIN),

    @SerializedName(value = "REPLACE", alternate = "replace")
    REPLACE("REPLACE", OneDBData.MethodType.REPLACE),

    @SerializedName(value = "MASK", alternate = "mask")
    MASK("MASK", OneDBData.MethodType.MASK),

    @SerializedName(value = "NUMBER_FLOOR", alternate = "{numberFloor, number_floor}")
    NUMBER_FLOOR("NUMBER_FLOOR", OneDBData.MethodType.NUMBER_FLOOR),

    @SerializedName(value = "DATE_FLOOR", alternate = "{dateFloor, date_floor}")
    DATE_FLOOR("NUMBER_FLOOR", OneDBData.MethodType.DATE_FLOOR);

    private final static ImmutableMap<Integer, MethodTypeWrapper> MAP;

    static {
        final ImmutableMap.Builder<Integer, MethodTypeWrapper> builder = ImmutableMap.builder();
        for (MethodTypeWrapper MethodTypeWrapper : values()) {
            builder.put(MethodTypeWrapper.getId(), MethodTypeWrapper);
        }
        MAP = builder.build();
    }

    private final OneDBData.MethodType methodType;
    private final String name;

    MethodTypeWrapper(String name, OneDBData.MethodType methodType) {
        this.name = name;
        this.methodType = methodType;
    }

    public static MethodTypeWrapper of(OneDBData.MethodType methodType) {
        return MAP.get(methodType.getNumber());
    }

    public String getName() {
        return name;
    }

    public OneDBData.MethodType get() {
        return methodType;
    }

    public int getId() {
        return methodType.getNumber();
    }

}
