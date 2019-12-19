package com.ilivoo.flume.source.jdbc;

import com.ilivoo.flume.jdbc.JDBCException;

public class JDBCSourceException extends JDBCException {

    private static final long serialVersionUID = 1L;

    public JDBCSourceException(String message) {
        super(message);
    }

    public JDBCSourceException(Throwable cause) {
        super(cause);
    }

    public JDBCSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
