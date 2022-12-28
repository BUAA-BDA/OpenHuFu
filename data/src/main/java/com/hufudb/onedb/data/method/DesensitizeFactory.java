package com.hufudb.onedb.data.method;

import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.proto.OneDBData;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DesensitizeFactory {

    public static Object implement(Object val, OneDBData.ColumnDesc columnDesc) {
        Object rt = val;
        OneDBData.Desensitize desensitize = columnDesc.getDesensitize();
        if (desensitize == null) {
            return rt;
        }
        OneDBData.Sensitivity sensitivity = desensitize.getSensitivity();
        OneDBData.Method method = desensitize.getMethod();
        PojoMethod pojoMethod = PojoMethod.fromColumnMethod(method);
        switch (sensitivity) {
            case PLAIN:
                break;
            case SENSITIVE:
                rt = pojoMethod.implement(val, columnDesc, method);
                break;
            case SECRET:

        }
        return rt;
    }

    public static void check(OneDBData.ColumnType localType, OneDBData.ColumnType desensitizationType, OneDBData.Method method) {
        PojoMethod pojoMethod = PojoMethod.fromColumnMethod(method);
        pojoMethod.check(localType, desensitizationType);
    }
}
