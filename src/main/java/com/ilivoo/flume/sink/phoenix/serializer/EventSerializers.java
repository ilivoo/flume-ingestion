package com.ilivoo.flume.sink.phoenix.serializer;

public enum EventSerializers {

    REGEX(RegexEventSerializer.class.getName()), JSON(JsonEventSerializer.class.getName()), CSV(CsvEventSerializer.class.getName());
    
    private final String className;
    
    private EventSerializers(String serializerClassName) {
        this.className = serializerClassName;
    }

    /**
     * @return Returns the serializer className.
     */
    public String getClassName() {
        return className;
    }
}