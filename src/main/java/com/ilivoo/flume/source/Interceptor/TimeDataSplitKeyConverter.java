package com.ilivoo.flume.source.Interceptor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TimeDataSplitKeyConverter extends TimeDataConverter {

    private static final Logger log = LoggerFactory.getLogger(TimeDataSplitKeyConverter.class);

    private static final int SPLIT_LENGTH = 2;

    private String split;

    private String splitKeyName;

    @Override
    protected List<Map<String, String>> doConvert(Map<String, String> element) {
        Map<String, Map<String, String>> splitMap = Maps.newHashMap();
        for (Map.Entry<String, String> entry : element.entrySet()) {
            String[] splitKeys  = entry.getKey().split(split, SPLIT_LENGTH);
            //todo
            if (splitKeys.length != 2) {
                continue;
            }
            Map<String, String> map = splitMap.get(splitKeys[0]);
            if (map == null) {
                map = Maps.newHashMap();
                map.put(splitKeyName, splitKeys[0]);
                splitMap.put(splitKeys[0], map);
            }
            map.put(splitKeys[1], entry.getValue());
        }
        return Lists.newArrayList(splitMap.values());
    }

    @Override
    public void configure(Context context) {
        super.configure(context);
        split = context.getString("split", "&");
        for (String key : keyMap.values()) {
            String[] splitStr = key.split(split, SPLIT_LENGTH);
            if (splitStr == null || splitStr.length != SPLIT_LENGTH) {
                log.error("key [{}] split length not {}", key, SPLIT_LENGTH);
                throw new FlumeException("key split length not " + SPLIT_LENGTH);
            }
            for (String str : splitStr) {
                if (Strings.isNullOrEmpty(str)) {
                    log.error("key [{}] split value is empty", key);
                    throw new FlumeException("key split value is empty");
                }
            }
        }
        splitKeyName = context.getString("splitKeyName");
        if (Strings.isNullOrEmpty(splitKeyName)) {
            log.error("split key name is empty");
            throw new FlumeException("split key name is empty");
        }
    }
}
