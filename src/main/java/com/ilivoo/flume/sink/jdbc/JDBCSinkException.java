package com.ilivoo.flume.sink.jdbc;

import com.ilivoo.flume.jdbc.JDBCException;

public class JDBCSinkException extends JDBCException {

    private static final long serialVersionUID = 1L;

    public JDBCSinkException(String message) {
        super(message);
    }

    public JDBCSinkException(Throwable cause) {
        super(cause);
    }

    public JDBCSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
