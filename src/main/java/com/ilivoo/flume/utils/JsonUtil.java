package com.ilivoo.flume.utils;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonUtil {

    public static final Gson gson = new Gson();

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
        JsonElement jsonElement = parser.parse(json);
        return jsonElementToStringMap(jsonElement);
    }

    public static List<Map<String, String>> jsonToStringMaps(String json) {
        List<Map<String, String>> result = Lists.newArrayList();
        JsonElement outerJsonElement = parser.parse(json);
        if (outerJsonElement.isJsonArray()) {
            Iterator<JsonElement> jsonElements = outerJsonElement.getAsJsonArray().iterator();
            while (jsonElements.hasNext()) {
                JsonElement jsonElement = jsonElements.next();
                result.add(jsonElementToStringMap(jsonElement));
            }
        } else {
            result.add(jsonElementToStringMap(outerJsonElement));
        }
        return result;
    }

    private static Map<String, String> jsonElementToStringMap(JsonElement jsonElement) {
        Map<String, String> result = new HashMap<>();
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

    public static String toJson(JsonElement jsonElement) {
        return gson.toJson(jsonElement);
    }

    public static String toJson(Object src) {
        return gson.toJson(src);
    }
}

