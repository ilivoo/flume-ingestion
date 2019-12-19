package com.ilivoo.flume.jdbc;

import javax.sql.DataSource;
import java.util.Properties;

public interface DataSourceProvider {

    public void configProps(Properties properties);

    public String getUrl();

    public DataSource createDataSource() throws Exception;
}
