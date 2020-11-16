package com.ilivoo.flume.sink.cache;

import java.util.Map;

public interface FlumeCache {

    void put(String name, String key, Map<String, String> value);

    Map<String, String> get(String name, String key);
}