package com.ilivoo.flume.source.Interceptor;

import com.google.common.collect.Lists;
import com.ilivoo.flume.utils.DateTimeUtil;
import com.ilivoo.flume.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeDataConverter extends KeyConverter {

    private static final Logger log = LoggerFactory.getLogger(TimeDataConverter.class);

    @Override
    public List<Map<String, String>> convert(byte[] payload) {
        Map<String, String> element = new HashMap<>();
        String json = new String(payload, StandardCharsets.UTF_8);
        log.debug("json: {}", json);
        TimeData timeData = null;
        try {
            timeData = JsonUtil.gson.fromJson(json, TimeData.class);
        } catch (Exception e) {
        }
        if (timeData == null
                || timeData.getData() == null
                || timeData.getData().size() == 0) {
            log.warn("can not convert json: {}", json);
            return null;
        }
        String convertTime = DateTimeUtil.parseToCanonical(timeData.getTime());
        if (convertTime == null) {
            log.warn("can not convert json: {}", json);
            return null;
        }
        for (NameValue nameValue : timeData.getData()) {
            String value = nameValue.getValue();
            String replaceName = nameValue.getName().replace(" ", "");
            String convertKey = keyMap.get(replaceName);
            if (convertKey != null) {
                element.put(convertKey, value);
            } else {
                element.put(nameValue.getName(), value);
            }
        }

        List<Map<String, String>> convertMap = doConvert(element);
        if (convertMap == null) {
            return null;
        }

        for (Map<String, String> map : convertMap) {
            map.put("originTime", timeData.getTime());
            map.put("time", convertTime);
        }
        log.debug("convert map {}", convertMap);
        return convertMap;
    }

    protected List<Map<String, String>> doConvert(Map<String, String> element) {
        return Lists.newArrayList(element);
    }

    private static class TimeData {
        private String time;

        private List<NameValue> Data;

        public String getTime() {
            return time;
        }

        public void setTime(String time) {
            this.time = time;
        }

        public List<NameValue> getData() {
            return Data;
        }

        public void setData(List<NameValue> data) {
            Data = data;
        }
    }

    private static class NameValue {
        private String name;

        private String value;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
