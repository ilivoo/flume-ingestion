package com.ilivoo.flume.sink.cache;

import java.util.Map;

public final class CacheHelper {
    private CacheHelper() {

    }

    private static FlumeCache flumeCache = new MemoryCache();

    public static void put(String name, String key, Map<String, String> value) {
        flumeCache.put(name, key, value);
    }

    public static Map<String, String> get(String name, String key) {
        return flumeCache.get(name, key);
    }
}
