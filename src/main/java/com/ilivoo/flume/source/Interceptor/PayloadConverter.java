package com.ilivoo.flume.source.Interceptor;

import org.apache.flume.NamedComponent;
import org.apache.flume.conf.Configurable;

import java.util.List;
import java.util.Map;

public abstract class PayloadConverter implements Configurable, NamedComponent {

    private String name;

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    public abstract List<Map<String, String>> convert(byte[] payload);
}
