package com.ilivoo.flume.source.Interceptor;

import com.google.common.collect.Lists;
import com.ilivoo.flume.utils.DateTimeUtil;
import com.ilivoo.flume.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Date;
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
        String convertTime = convertTime(timeData.getTime());
        if (convertTime == null) {
            log.warn("can not convert json: {}", json);
            return null;
        }
        element.put("originTime", timeData.getTime());
        element.put("time", convertTime);
        for (NameValue nameValue : timeData.getData()) {
            //todo
            String value = nameValue.getValue();
            if (value.equals("timeout") || value.equals("error")) {
                log.warn("json contain timeout or error value, drop it");
                return null;
            }
            String replaceName = nameValue.getName().replace(" ", "");
            String convertKey = keyMap.get(replaceName);
            if (convertKey != null) {
                element.put(convertKey, value);
            } else {
                element.put(nameValue.getName(), value);
            }
        }
        log.debug("convert map {}", element);
        return Lists.newArrayList(element);
    }

    private String convertTime(String time) {
        String cTime;
        try {
            long convertTime = DateTimeUtil.parseDateTimeString(time, null);
            cTime = DateTimeUtil.DEFAULT_FORMAT.get().format(new Date(convertTime));
        } catch (Exception e) {
            StringBuilder timeBuilder = new StringBuilder();
            String[] dateTime = time.split(" ", 2);
            //year month day
            String[] ymd = dateTime[0].split("-", 3);
            if (ymd[0].length() == 2) {//year
                timeBuilder.append("20");
            }
            timeBuilder.append(ymd[0]).append("-");

            if (ymd[1].length() == 1) { //month
                timeBuilder.append("0");
            }
            timeBuilder.append(ymd[1]).append("-");

            if (ymd[2].length() == 1) { //day
                timeBuilder.append("0");
            }
            timeBuilder.append(ymd[2]).append(" ");
            //hour minute second
            String[] hms = dateTime[1].split(":", 3);
            if (hms[0].length() == 1) { //hour
                timeBuilder.append("0");
            }
            timeBuilder.append(hms[0]).append(":");

            if (hms[1].length() == 1) { // minute
                timeBuilder.append("0");
            }
            timeBuilder.append(hms[1]).append(":");
            if (hms[2].length() == 1) {
                timeBuilder.append("0");
            }
            timeBuilder.append(hms[2]);
            cTime = timeBuilder.toString();
            if (cTime.length() != 19) {//yyyy-MM-dd hh:mm:ss
                return null;
            }
        }
        return cTime;
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
