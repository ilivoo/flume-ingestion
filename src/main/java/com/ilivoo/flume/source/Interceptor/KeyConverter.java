package com.ilivoo.flume.source.Interceptor;

import com.google.common.base.Strings;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;

import java.util.HashMap;
import java.util.Map;

public abstract class KeyConverter extends PayloadConverter {

    protected Map<String, String> keyMap = new HashMap<>();

    @Override
    public void configure(Context context) {
        String keyListStr = context.getString("keys");
        if (Strings.isNullOrEmpty(keyListStr)) {
            throw new FlumeException("keys not exist");
        }
        String[] keyNames = keyListStr.split("\\s+");

        if (keyNames.length <= 0) {
            throw new FlumeException("keys not exist");
        }

        Context keyContexts = new Context(context.getSubProperties("keys."));

        for (String keyName : keyNames) {
            String keyValue = keyContexts.getString(keyName);
            if (Strings.isNullOrEmpty(keyValue)) {
                throw new FlumeException("key: " + keyName + ", value not exist");
            }
            String replaceValue = keyValue.replaceAll(" ", "");
            keyMap.put(replaceValue, keyName);
        }
    }
}
