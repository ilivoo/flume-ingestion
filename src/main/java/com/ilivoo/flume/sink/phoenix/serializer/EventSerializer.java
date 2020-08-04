package com.ilivoo.flume.sink.phoenix.serializer;

import java.sql.SQLException;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;

import org.apache.phoenix.util.SQLCloseable;

public interface EventSerializer extends Configurable,ConfigurableComponent,SQLCloseable {

    /**
     * called during the start of the process to initialize the table columns.
     */
    public void initialize() throws SQLException;
    
    /**
     * @param events to be written to HBase.
     * @throws SQLException 
     */
    public void upsertEvents(List<Event> events) throws SQLException;
    
}
