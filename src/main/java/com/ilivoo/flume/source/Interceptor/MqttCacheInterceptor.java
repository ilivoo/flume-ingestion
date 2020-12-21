package com.ilivoo.flume.source.Interceptor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.utils.Utils;
import com.ilivoo.flume.sink.cache.CacheHelper;
import com.ilivoo.flume.utils.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MqttCacheInterceptor extends MqttInterceptor {

    private static final String CACHE_NAME = "cacheName";
    private static final String CACHE_INCLUDES = "cacheIncludes";

    private static final String CACHE_CHECK_INTERVAL = "cacheCheckInterval";

    private static final String CONVERTER_KEY = "converterKey";

    private static final String ANONYMOUS = "<Anonymous>";


    private static final long DEFAULT_CACHE_CHECK_INTERVAL = 10;

    private String cacheName;

    private String[] cacheIncludes;

    private long cacheCheckInterval;

    private String converterKey;

    //topic filter
    private int topicPosition = -1;
    private String topicKey;
    //cache filter
    private String cacheValue;
    private String cacheKey;

    private Map<String, Expression> transMap = new HashMap<>();

    @Override
    protected Event mqttIntercept(KafkaRecordInfo recordInfo, MqttPublishInfo publishInfo) {
        String clientId = publishInfo.clientId();
        if (ANONYMOUS.equals(clientId)) {
            return null;
        }
        Map<String, String> cacheValue = CacheHelper.get(cacheName, clientId);
        long waitTime = 0;
        while (cacheValue == null) {
            try {
                LOG.info("cache value not exist, name {}, key {}, wait time {} second", cacheName, clientId, waitTime / 1000);
                Thread.sleep(cacheCheckInterval);
                cacheValue = CacheHelper.get(cacheName, clientId);
                waitTime += cacheCheckInterval;
                LOG.info("publish info, topic [{}], user properties [{}]", publishInfo.topic(), publishInfo.userProperties());
            } catch (InterruptedException e) {
                throw new FlumeException("wait cache interrupted exception");
            }
        }
        return cacheMqttIntercept(recordInfo, publishInfo, cacheValue);
    }

    protected Event cacheMqttIntercept(KafkaRecordInfo recordInfo, MqttPublishInfo publishInfo, Map<String, String> deviceInfo) {
        LOG.debug("interceptor message from client id: {}", recordInfo.key());
        if (topicPosition != -1) { ;
            LOG.debug("mqtt topic {}", publishInfo.topic());
            String[] topicSplit = publishInfo.topic().split("/");
            if (topicPosition >= topicSplit.length) {
                LOG.error("Topic {} has no position {}", publishInfo.topic(), topicPosition);
                throw new RuntimeException("Topic has no position");
            }
            String value = deviceInfo.get(topicKey);
            String positionValue = topicSplit[topicPosition];
            LOG.debug("topic position [{}] value [{}], key [{}] value [{}]", topicPosition, positionValue, topicKey, value);
            if (!positionValue.equals(value)) {
                LOG.warn("topic position [{}] value [{}] is not equals key [{}] value [{}]", topicPosition, positionValue, topicKey, value);
                return null;
            }
        }
        if (!Strings.isNullOrEmpty(cacheValue)) {
            String value = deviceInfo.get(cacheKey);
            if (Strings.isNullOrEmpty(value)) {
                LOG.error("Client [{}], cache key [{}] value is null", publishInfo.clientId(), cacheKey);
                throw new RuntimeException("Client [{}], cache key [{}] value is null");
            }
            if (!cacheValue.equals(value)) {
                LOG.debug("drop event, cache value [{}], expect value [{}]", value, cacheValue);
                return null;
            }
        }

        String converterName = deviceInfo.get(converterKey);
        PayloadConverter payloadConverter = converterMap.get(converterName);
        if (payloadConverter == null) {
            throw new FlumeException("can not find converter, name: " + converterName);
        }
        List<Map<String, String>> payloads = payloadConverter.convert(publishInfo.payload());
        if (payloads == null || payloads.size() == 0) {
            return null;
        }

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

        for (Map<String, String> payload : payloads) {
            Map<String, Object> env = Maps.transformValues(payload, new Function<String, Object>() {
                @Override
                public Object apply(@Nullable String input) {
                    return input;
                }
            });
            Map<String, Object> modifyEnv = new HashMap<>();
            modifyEnv.putAll(env);
            for (Map.Entry<String, Expression> entry : transMap.entrySet()) {
                Object transValue;
                try {
                    transValue = entry.getValue().execute(modifyEnv);
                } catch (Exception e) {
                    LOG.warn("script [{}] execute error, env {}", entry.getKey(), modifyEnv);
                    throw new RuntimeException(e);
                }
                if (transValue != null) {
                    payload.put(entry.getKey(), transValue.toString());
                }
            }
        }

        LOG.debug("payloads {}", payloads);

        Object events;

        if (payloads.size() == 1) {
            events = payloads.get(0);
        } else {
            events = payloads;
        }
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("key", recordInfo.key());
        headerMap.putAll(deviceInfo);
        return EventBuilder.withBody(JsonUtil.toJson(events), StandardCharsets.UTF_8, headerMap);
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

        String filters = context.getString("filters");
        if (!Strings.isNullOrEmpty(filters)) {
            String[] filterNames = filters.split("\\s+");
            Set<String> filterSet = Sets.newHashSet(filterNames);
            //a1.sources.r2.interceptors.i1.filters = topic
            //a1.sources.r2.interceptors.i1.filters.topic.position = 4
            //a1.sources.r2.interceptors.i1.filters.topic.key = device_code
            if (filterSet.contains("topic")) {
                Context topicFilterContext = new Context(context.getSubProperties("filters.topic."));
                topicPosition = topicFilterContext.getInteger("position", -1);
                topicKey = topicFilterContext.getString("key");
            }
            //a1.sources.r2.interceptors.i1.filters = cache
            //a1.sources.r2.interceptors.i1.filters.cache.value = shpdnw
            //a1.sources.r2.interceptors.i1.filters.cache.key = app_code
            if (filterSet.contains("cache")) {
                Context topicFilterContext = new Context(context.getSubProperties("filters.cache."));
                cacheValue = topicFilterContext.getString("value");
                cacheKey = topicFilterContext.getString("key");
                if (!Strings.isNullOrEmpty(cacheValue) && Strings.isNullOrEmpty(cacheKey)) {
                    throw new FlumeException("filter: cache value not empty, but cache key is null");
                }
            }
        }

        String transforms = context.getString("transforms");
        if (!Strings.isNullOrEmpty(transforms)) {
            String[] transNames = transforms.split("\\s+");
            Context transformContexts = new Context(context.getSubProperties("transforms."));
            for (String transName : transNames) {
                String transScript = transformContexts.getString(transName);
                if (Strings.isNullOrEmpty(transScript)) {
                    throw new FlumeException("transform: " + transName + ", expression not exist");
                }
                String stripScript = StringUtils.strip(transScript);
                Expression expression;
                try {
                    InputStream in = new FileInputStream(stripScript);
                    Reader reader = new InputStreamReader(in, Charset.forName("utf-8"));
                    String scriptContent = Utils.readFully(reader);
                    LOG.debug("script content: {}", scriptContent);

                    expression = AviatorEvaluator.compile(scriptContent, true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                transMap.put(transName, expression);
            }
        }
    }

    @Override
    public void initialize() {

    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {
        String script = "app_code == '6' + '2' ? long(a)+long(b) : nil";
//        String script = "long(a) + double(b)";
        Map<String, Object> env = new HashMap<>();
        env.put("app_code", "62");
        env.put("a", "1");
        env.put("b", "2");
        Expression expression = AviatorEvaluator.compile(script, true);
        Object result = expression.execute(env);
        String resultType = result.getClass().getSimpleName();
        System.out.println(resultType + ": " + result);


        String topic = "/Topic/1/2/3/";
        String[] topicSplit = topic.split("/");
        System.out.println(Arrays.asList(topicSplit));
    }
}
