package com.ilivoo.flume.sink.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryCache implements FlumeCache {

    private Map<String, Map<String, Map<String, String>>> nameCache = new ConcurrentHashMap<>();

    @Override
    public void put(String name, String key, Map<String, String> value) {
        Map<String, Map<String, String>> keyCache = nameCache.get(name);
        if (keyCache == null) {
            keyCache = new ConcurrentHashMap<>();
            Map<String, Map<String, String>> oldKeyCache = nameCache.put(name, keyCache);
            if (oldKeyCache != null) {
                keyCache = oldKeyCache;
            }
        }
        keyCache.put(key, value);
    }

    @Override
    public Map<String, String> get(String name, String key) {
        Map<String, Map<String, String>> keyCache = nameCache.get(name);
        if (keyCache == null) {
            return null;
        }
        Map<String, String> value = keyCache.get(key);
        return value;
    }
}
