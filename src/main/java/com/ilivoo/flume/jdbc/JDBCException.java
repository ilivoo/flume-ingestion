package com.ilivoo.flume.jdbc;

public class JDBCException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JDBCException(String message) {
        super(message);
    }

    public JDBCException(Throwable cause) {
        super(cause);
    }

    public JDBCException(String message, Throwable cause) {
        super(message, cause);
    }
}
