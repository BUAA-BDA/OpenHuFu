package com.hufudb.onedb.data.schema.utils;

import com.hufudb.onedb.data.desensitize.utils.Maintain;
import com.hufudb.onedb.data.desensitize.utils.Replace;
import com.hufudb.onedb.data.desensitize.utils.Mask;
import com.hufudb.onedb.data.desensitize.utils.DateFloor;
import com.hufudb.onedb.data.desensitize.utils.NumberFloor;
import com.hufudb.onedb.data.storage.utils.MethodTypeWrapper;
import com.hufudb.onedb.proto.OneDBData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PojoMethod {
    public MethodTypeWrapper type;
    protected List<OneDBData.ColumnType> allowedTypes = new ArrayList<>();
    private static final Logger LOG = LoggerFactory.getLogger(PojoMethod.class);

    public PojoMethod(MethodTypeWrapper type) {
        this.type = type;
    }

    public static PojoMethod fromColumnMethod(OneDBData.Method method) {
        switch (method.getMethodCase()) {
            case REPLACE:
                return new Replace(MethodTypeWrapper.REPLACE, method.getReplace().getFromStr(), method.getReplace().getToStr());
            case MASK:
                return new Mask(MethodTypeWrapper.MASK, method.getMask().getBegin(), method.getMask().getEnd(), method.getMask().getStr());
            case NUMBER_FLOOR:
                return new NumberFloor(MethodTypeWrapper.NUMBER_FLOOR, method.getNumberFloor().getPlace());
            case DATE_FLOOR:
                return new DateFloor(MethodTypeWrapper.Date_FLOOR, method.getDateFloor().getFloor());
            case MAINTAIN:
            default:
                return new Maintain(MethodTypeWrapper.MAINTAIN);
        }
    }

    public OneDBData.Method toMethod() {
        return null;
    }

    public MethodTypeWrapper getType() {
        return type;
    }

    public void setType(MethodTypeWrapper type) {
        this.type = type;
    }

    public static PojoMethod methodDefault() {
        return PojoMethod.fromColumnMethod(OneDBData.Method.newBuilder().build());
    }

    public Object implement(Object val, OneDBData.ColumnDesc columnDesc, OneDBData.Method method) {
        return null;        
    }

    public void check(OneDBData.ColumnType localType, OneDBData.ColumnType desensitizationType) {
        if (desensitizationType != OneDBData.ColumnType.UNKNOWN && desensitizationType != localType) {
            LOG.error("LocalType {} and DesensitizationType {} is not same", localType.toString(), desensitizationType.toString());
        }
        if (!allowedTypes.contains(localType) && !(this instanceof Maintain)) {
            StringBuilder except = new StringBuilder();
            except.append("|");
            for (OneDBData.ColumnType allowedType : allowedTypes) {
                except.append(allowedType.toString()).append("|");
            }
            String className = this.getClass().getName();
            String[] tmp = className.split("\\.");
            className = tmp[tmp.length-1];
            LOG.error("Desensitization Method {} don't match Type {}, Type except to be {}", className, localType.toString(), except.toString());
        }
    }

}
