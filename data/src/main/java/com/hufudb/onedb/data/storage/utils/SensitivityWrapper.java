package com.hufudb.onedb.data.storage.utils;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.hufudb.onedb.proto.OneDBData;
import com.hufudb.onedb.proto.OneDBData.Sensitivity;

public enum SensitivityWrapper {

    @SerializedName(value = "PLAIN", alternate = "plain")
    PLAIN(Sensitivity.PLAIN),
    @SerializedName(value = "SENSITIVE", alternate = "sensitive")
    SENSITIVE(Sensitivity.SENSITIVE),
    @SerializedName(value = "SECRET", alternate = "secret")
    SECRET(Sensitivity.SECRET);

    private final static ImmutableMap<Integer, SensitivityWrapper> MAP;

    static {
        final ImmutableMap.Builder<Integer, SensitivityWrapper> builder = ImmutableMap.builder();
        for (SensitivityWrapper SensitivityWrapper : values()) {
            builder.put(SensitivityWrapper.getId(), SensitivityWrapper);
        }
        MAP = builder.build();
    }

    private final Sensitivity sensitivity;

    SensitivityWrapper(Sensitivity sensitivity) {
        this.sensitivity = sensitivity;
    }

    public Sensitivity get() {
        return sensitivity;
    }

    public int getId() {
        return this.sensitivity.getNumber();
    }

    public static SensitivityWrapper of(OneDBData.Sensitivity sensitivity) {
        return MAP.get(sensitivity.getNumber());
    }
}
