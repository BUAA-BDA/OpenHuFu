package com.hufudb.onedb.data.desensitize;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.hufudb.onedb.data.desensitize.utils.*;
import com.hufudb.onedb.data.schema.utils.PojoMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

public class MethodDeserializer implements JsonDeserializer<PojoMethod> {

    private static final Logger LOG = LoggerFactory.getLogger(MethodDeserializer.class);
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
                case "DATE_FLOOR":
                    return context.deserialize(json, DateFloor.class);
                default:
                    LOG.error("Don't have {} desensitize method!!", type);
                    return null;
            }
        }
    }
}