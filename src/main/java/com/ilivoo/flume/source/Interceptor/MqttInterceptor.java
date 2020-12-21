package com.ilivoo.flume.source.Interceptor;

import com.google.common.base.Strings;
import com.ilivoo.flume.utils.JsonUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.apache.flume.source.kafka.KafkaSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link KafkaSourceConstants} flume event header
 */
public abstract class MqttInterceptor implements Interceptor, Builder {

    protected static final Logger LOG = LoggerFactory.getLogger(MqttInterceptor.class);

    protected Map<String, PayloadConverter> converterMap = new HashMap<>();

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        final Map<String, String> header = event.getHeaders();
        KafkaRecordInfo kafkaRecordInfo = new KafkaRecordInfo() {
            @Override
            public String topic() {
                return header.get("topic");
            }

            @Override
            public int partition() {
                return Integer.valueOf(header.get("partition"));
            }

            @Override
            public long offset() {
                return Long.valueOf(header.get("offset"));
            }

            @Override
            public String key() {
                return header.get("key");
            }

            @Override
            public long timestamp() {
                return Long.valueOf(header.get("timestamp"));
            }
        };

        String kafkaValue = new String(event.getBody(), StandardCharsets.UTF_8);
        LOG.debug("kafka value: {}", kafkaValue);
        final Map<String, Object> mqttInfo = JsonUtil.jsonToObjectMap(kafkaValue);
        MqttPublishInfo mqttPublishInfo = new MqttPublishInfo() {
            @Override
            public String clientId() {
                return (String) mqttInfo.get("clientId");
            }

            @Override
            public int qos() {
                return (Integer) mqttInfo.get("clientId");
            }

            @Override
            public boolean retain() {
                return (Boolean) mqttInfo.get("retain");
            }

            @Override
            public String topic() {
                return (String) mqttInfo.get("topic");
            }

            @Override
            public Integer payloadFormatIndicator() {
                Object indicator = mqttInfo.get("payloadFormatIndicator");
                return indicator == null ? null : (Integer) indicator;
            }

            @Override
            public Integer messageExpiryInterval() {
                Object interval = mqttInfo.get("messageExpiryInterval");
                return interval == null ? null : (Integer) interval;
            }

            @Override
            public String responseTopic() {
                Object topic = mqttInfo.get("responseTopic");
                return topic == null ? null : (String) topic;
            }

            @Override
            public byte[] correlationData() {
                Object correlation = mqttInfo.get("correlationData");
                return correlation == null ? null : Base64.decodeBase64((String) correlation);
            }

            @Override
            public String contentType() {
                Object type = mqttInfo.get("contentType");
                return type == null ? null : (String) type;
            }

            @Override
            public Map<String, String> userProperties() {
                Object properties = mqttInfo.get("userProperties");
                Map<String, String> result = null;
                if (properties != null) {
                    result = JsonUtil.jsonToStringMap((String) properties);
                }
                return result;
            }

            @Override
            public byte[] payload() {
                Object payload = mqttInfo.get("payload");
                return payload == null ? null : Base64.decodeBase64((String) payload);
            }

            @Override
            public Map<String, Object> origin() {
                return mqttInfo;
            }
        };
        return mqttIntercept(kafkaRecordInfo, mqttPublishInfo);
    }

    protected abstract Event mqttIntercept(KafkaRecordInfo recordInfo, MqttPublishInfo publishInfo);

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> newEvents = new ArrayList<>();
        for (Event event : events) {
            Event newEvent = intercept(event);
            if (newEvent != null) {
                newEvents.add(newEvent);
            }
        }
        return newEvents;
    }

    interface KafkaRecordInfo {
        String topic();

        int partition();

        long offset();

        String key();

        long timestamp();
    }

    interface MqttPublishInfo {
        @NotNull
        String clientId();

        int qos();

        boolean retain();

        @NotNull
        String topic();

        Integer payloadFormatIndicator();

        Integer messageExpiryInterval();

        String responseTopic();

        byte[] correlationData();

        String contentType();

        Map<String, String> userProperties();

        byte[] payload();

        Map<String, Object> origin();
    }

    @Override
    public Interceptor build() {
        return this;
    }

    @Override
    public void configure(Context context) {

        String converterListStr = context.getString("converters");
        if (Strings.isNullOrEmpty(converterListStr)) {
            throw new FlumeException("converters not exist");
        }
        String[] converterNames = converterListStr.split("\\s+");

        if (converterNames.length <= 0) {
            throw new FlumeException("converters not exist");
        }

        Context converterContexts = new Context(context.getSubProperties("converters."));

        // run through and instantiate all the interceptors specified in the Context
        for (String converterName : converterNames) {
            Context converterContext = new Context(
                    converterContexts.getSubProperties(converterName + "."));
            String type = converterContext.getString("type");
            if (type == null) {
                LOG.error("Type not specified for converter" + converterName);
                throw new FlumeException("Converter not specified for " +
                        converterName);
            }
            try {
                Class<? extends PayloadConverter> clazz = (Class<? extends PayloadConverter>) Class.forName(type);
                PayloadConverter payloadConverter = clazz.newInstance();
                payloadConverter.setName(converterName);
                payloadConverter.configure(converterContext);
                converterMap.put(payloadConverter.getName(), payloadConverter);
            } catch (ClassNotFoundException e) {
                LOG.error("Builder class not found. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not found.", e);
            } catch (InstantiationException e) {
                LOG.error("Could not instantiate Builder. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not constructable.", e);
            } catch (IllegalAccessException e) {
                LOG.error("Unable to access Builder. Exception follows.", e);
                throw new FlumeException("Unable to access Interceptor.Builder.", e);
            }
        }
    }
}
