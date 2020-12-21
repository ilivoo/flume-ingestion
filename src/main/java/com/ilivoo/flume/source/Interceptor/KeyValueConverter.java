package com.ilivoo.flume.source.Interceptor;

import com.google.common.collect.Lists;
import com.ilivoo.flume.utils.DateTimeUtil;
import com.ilivoo.flume.utils.JsonUtil;
import joptsimple.internal.Strings;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class KeyValueConverter extends PayloadConverter {

    private static final Logger log = LoggerFactory.getLogger(KeyValueConverter.class);

    private final String TIME_KEY = "time";

    @Override
    public List<Map<String, String>> convert(byte[] payload) {
        Map<String, String> element = null;
        String json = new String(payload, StandardCharsets.UTF_8);
        log.debug("json: {}", json);
        try {
            element = JsonUtil.jsonToStringMap(json);
        } catch (Exception e) {
        }
        if (element == null || element.size() == 0) {
            log.warn("json parse error, json: {}", json);
            return null;
        }

        String time = element.get(TIME_KEY);
        if (Strings.isNullOrEmpty(time)) {
            log.warn("time key is empty, json: {}", json);
            return null;
        }

        String convertTime = DateTimeUtil.parseToCanonical(time);
        if (convertTime == null) {
            log.warn("time parse error, json: {}", json);
            return null;
        }
        element.put("originTime", time);
        element.put(TIME_KEY, convertTime);
        log.debug("convert map {}", element);
        return Lists.newArrayList(element);
    }

    @Override
    public void configure(Context context) {

    }
}
