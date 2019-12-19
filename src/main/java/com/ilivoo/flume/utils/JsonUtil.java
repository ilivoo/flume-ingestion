package com.ilivoo.flume.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.Map;

public class JsonUtil {

    private static final JsonParser parser = new JsonParser();

    public static Map<String, Object> jsonToObjectMap(String json) {
        Map<String, Object> result = new HashMap<>();
        JsonElement jsonElement = parser.parse(json);
        for (Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
            String key = entry.getKey();
            JsonElement value = entry.getValue();
            if (value.isJsonPrimitive()) {
                JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (primitive.isBoolean()) {
                    result.put(key, primitive.getAsBoolean());
                } else if (primitive.isString()) {
                    result.put(key, primitive.getAsString());
                } else if (primitive.isNumber()) {
                    result.put(key, primitive.getAsNumber());
                }
            } else if (value.isJsonObject() || value.isJsonArray()) {
                result.put(entry.getKey(), value.toString());
            } else if (value.isJsonNull()) {
                //do nothing
            }
        }
        return result;
    }

    public static Map<String, String> jsonToStringMap(String json) {
        Map<String, String> result = new HashMap<>();
        JsonElement jsonElement = parser.parse(json);
        for (Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
            String key = entry.getKey();
            JsonElement value = entry.getValue();
            if (value.isJsonPrimitive()) {
                JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (primitive.isBoolean()) {
                    result.put(key, primitive.getAsString());
                } else if (primitive.isString()) {
                    result.put(key, primitive.getAsString());
                } else if (primitive.isNumber()) {
                    result.put(key, primitive.getAsString());
                }
            } else if (value.isJsonObject() || value.isJsonArray()) {
                result.put(entry.getKey(), value.toString());
            } else if (value.isJsonNull()) {
                //do nothing
            }
        }
        return result;
    }
}

