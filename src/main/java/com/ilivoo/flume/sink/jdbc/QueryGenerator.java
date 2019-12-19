package com.ilivoo.flume.sink.jdbc;

import org.apache.flume.Event;
import org.jooq.DSLContext;

import java.util.List;

interface QueryGenerator {

    boolean executeQuery(DSLContext context, List<Event> event) throws Exception;
}
