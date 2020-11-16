package com.ilivoo.flume.source.Interceptor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ilivoo.flume.sink.cache.CacheHelper;
import com.ilivoo.flume.utils.JsonUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MqttCacheInterceptor extends MqttInterceptor {

    private static final String CACHE_NAME = "cacheName";
    private static final String CACHE_INCLUDES = "cacheIncludes";

    private static final String CACHE_CHECK_INTERVAL = "cacheCheckInterval";

    private static final String CONVERTER_KEY = "converterKey";


    private static final long DEFAULT_CACHE_CHECK_INTERVAL = 10;

    private String cacheName;

    private String[] cacheIncludes;

    private long cacheCheckInterval;

    private String converterKey;

    @Override
    protected Event mqttIntercept(KafkaRecordInfo recordInfo, MqttPublishInfo publishInfo) {
        String clientId = publishInfo.clientId();
        Map<String, String> cacheValue = CacheHelper.get(cacheName, clientId);
        while (cacheValue == null) {
            try {
                LOG.debug("cache value not exist, name {}, key {}, wait error", cacheName, clientId);
                Thread.sleep(cacheCheckInterval);
                cacheValue = CacheHelper.get(cacheName, clientId);
            } catch (InterruptedException e) {
                throw new FlumeException("wait cache interrupted exception");
            }
        }
        return cacheMqttIntercept(recordInfo, publishInfo, cacheValue);
    }

    protected Event cacheMqttIntercept(KafkaRecordInfo recordInfo, MqttPublishInfo publishInfo, Map<String, String> deviceInfo) {
        String converterName = deviceInfo.get(converterKey);
        PayloadConverter payloadConverter = converterMap.get(converterName);
        if (payloadConverter == null) {
            throw new FlumeException("can not find converter, name: " + converterName);
        }
        List<Map<String, String>> payloads = payloadConverter.convert(publishInfo.payload());
        if (payloads == null || payloads.size() == 0) {
            return null;
        }

        LOG.debug("payloads {}", payloads);

        if (cacheIncludes != null && cacheIncludes.length > 0) {
            for (Map<String, String> payload : payloads) {
                for (String key : cacheIncludes) {
                    String value = deviceInfo.get(key);
                    if (!Strings.isNullOrEmpty(value)) {
                        payload.put(key, value);
                    }
                }
            }
        } else {
            for (Map<String, String> payload : payloads) {
                payload.putAll(deviceInfo);
            }
        }


        Object events;

        if (payloads.size() == 1) {
            events = payloads.get(0);
        } else {
            events = payloads;
        }
        return EventBuilder.withBody(JsonUtil.toJson(events), StandardCharsets.UTF_8, new HashMap<String, String>());
    }

    @Override
    public void configure(Context context) {
        super.configure(context);

        this.cacheName = context.getString(CACHE_NAME);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(cacheName));

        String cacheIncludesListStr = context.getString(CACHE_INCLUDES);
        if (!Strings.isNullOrEmpty(cacheIncludesListStr)) {
            cacheIncludes = cacheIncludesListStr.split("\\s+");
        }

        this.cacheCheckInterval = context.getLong(CACHE_CHECK_INTERVAL, DEFAULT_CACHE_CHECK_INTERVAL) * 1000;
        Preconditions.checkArgument(!Strings.isNullOrEmpty(cacheName));

        converterKey = context.getString(CONVERTER_KEY);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(converterKey));
    }

    @Override
    public void initialize() {

    }

    @Override
    public void close() {

    }
}
