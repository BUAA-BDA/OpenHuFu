package com.hufudb.onedb.data.method;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.hufudb.onedb.data.schema.utils.PojoMethod;

import java.lang.reflect.Type;

public class MethodDeserializer implements JsonDeserializer<PojoMethod> {
    @Override
    public PojoMethod deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        if (json == null) {
            return null;
        } else  {
            String type = json.getAsJsonObject().get("type").getAsString();
            switch (type) {
                case "MAINTAIN":
                    return context.deserialize(json, Maintain.class);
                case "REPLACE":
                    return context.deserialize(json, Replace.class);
                case "MASK":
                    return context.deserialize(json, Mask.class);
                case "NUMBER_FLOOR":
                    return context.deserialize(json, NumberFloor.class);
                default:
                    return null;
            }
        }
    }
}