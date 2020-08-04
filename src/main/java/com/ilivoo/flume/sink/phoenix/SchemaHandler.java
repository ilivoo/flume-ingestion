package com.ilivoo.flume.sink.phoenix;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class SchemaHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaHandler.class);

    public static boolean createTable(Connection connection, String createTableDdl) {
        Preconditions.checkNotNull(connection);
        Preconditions.checkNotNull(createTableDdl); 
        boolean status  = true;
        try {
            status = connection.createStatement().execute(createTableDdl);
        } catch (SQLException e) {
            logger.error("An error occurred during executing the create table ddl {} ",createTableDdl);
            Throwables.propagate(e);
        }
        return status;
        
    }

}
