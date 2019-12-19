## flume-ingestion

flume-ingestion是一个Flume的插件，支持使用JDBC从关系型数据库中获取数据并写入关系型数据中，目前支持常见的数据库MySQL、SQLServer、Oracle、PostgreSQL等常见的数据库，底层数据库的操作依赖于[JOOQ](https://www.jooq.org/)， 如果需要使用SQLServer、Oracle等商业数据库的支持，请下载JOOQ商业版并更改依赖。

## 开发状态

flume-ingestion目前已经在线上部署使用，但并未进行非常全面的测试，如需要线上部署可自行进行充分测试。

## 技术说明

- flume-ingestion分为source端和sink端。

- source端通过增量的方式获取数据，目前支持两种增量方式，单字段自增和双字段（标识字段和自增字段）自增，自增字段可以是常见的数值、字符串、日期类型，标识字段可以是所有支持比较的类型。如需无唯一自增字段同步，需要自行构建自增字段，或者保证批次获取数量大于自增字段的单批次数量。
- sink端通过获取json数据，将json对象转换成关系型数据库的行并写入表中，目前支持直接SQL方式和对象映射方式。
- source和sink端都使用关系型数据库中的事务进行操作，所以为了保证数据完整性需要保证Flume的正常关闭和启动。

### 使用示例

- 所有的配置参数沿用flume标准的配置

- 公共参数，source和sink端都可以配置

  - 对于使用的数据库版本来说需要添加指定的驱动到flume classpath当中。

  - conn 用来指定数据库连接相关信息，如果没有声明conn.provider，默认使用 [HikariCP](https://github.com/brettwooldridge/HikariCP) 链接池，如：

    ```
    conn.jdbcUrl = jdbc:sqlserver://localhost:1433;DatabaseName=DB
    conn.maximumPoolSize = 1
    conn.dataSource.user = sa
    conn.dataSource.password = password
    ```

    通常情况下 HikariCP 能够正常工作，但对于版本非常低的数据库兼容性并不友好，所以提供了[DBCP 1.4](https://commons.apache.org/proper/commons-dbcp/)进行操作。

    ```
    conn.provider = com.ilivoo.flume.jdbc.DBCPDataSourceProvider
    conn.driverClassName = com.microsoft.jdbc.sqlserver.SQLServerDriver
    conn.url = jdbc:microsoft:sqlserver://localhost:1433;DatabaseName=DB
    conn.username = sa
    conn.password = password
    ```

  - tables 参数用来指定需要操作的一个或多个数据表，对于source端必须指定，sink端可选，指定的表名一定是数据库中的表名，用来进行表级别的权限限制，对于单个表名可以对其设置别名，对于source端别名表示写入到Channel中的表名，sink端别名表示读取Channel中的表名。

    ```
    tables = table1 table2 
    tables.table1 = table1_alias
    ```

  - columns 参数用来指定可以操作的一个或多个字段，source和sink端都是可选，指定列名一定是数据库中的列名，用来进行列级别的权限限制，对于单个列名可以对其设置别名，对于source端别名表示写入到Channel中的列名，sink端列名表示读取Channel中的列名。

  - ```
    tables.table1.columns = column1 column2
    tables.table1.columns.column1 = column1_alias
    ```

  - batchSize 参数用来指定批次操作的大小，source端表示每次读取自增字段的行数，sink端表示每次写入表中的行数。

- source端

  - 单字段自增

    ```
    #use jdbcsource
    type = com.ilivoo.flume.source.jdbc.JDBCSource
    #connect
    conn.jdbcUrl = jdbc:mysql://127.0.0.1:3306/test?useSSL=false
    conn.maximumPoolSize = 1
    conn.dataSource.user = root
    conn.dataSource.password = root
    #table idle
    idleMax = 60000
    idleInterval = 1000
    #read batch size
    batchSize = 100
    #offset dir
    positionDir = .flume
    #limit table
    tables = pptn cmsw
    tables.pptn = pptn_rename
    #table level idle
    tables.pptn.idleMax = 1800000
    tables.pptn.idleInterval = 30000
    #limit column
    tables.pptn.columns = field1 field2 field3
    tables.pptn.columns.field1 = field1_rename
    #increment
    tables.pptn.increments = autoId
    tables.pptn.increments.defaultStart = 1000
    ```

    - idleMax、idleInterval表示当没有获取到数据时闲置的最大时间和闲置间隔，闲置间隔通过指数的方式进行递增，到达最大闲置时间时会再次发出查询请求，也可以对单独的表进行分别设置不同闲置时间。
    - positionDir用来存储读取数据的偏移量。
    - increments 用来指定自增字段，单字段自增只需要设置一个
    - increments.defaultStart 用来指定自增字段的起始位置，对于数值型默认起始位置为0，字符串默认起始位置为空字符串，日期类型默认起始位置为 1970-01-01 00:00:00。

  - 双字段自增，双字段自增第一个字段是标识符字段，第二个字段为自增字段

    ```
    type = com.ilivoo.flume.source.jdbc.JDBCSource
    conn.provider = com.ilivoo.flume.jdbc.DBCPDataSourceProvider
    conn.driverClassName = com.microsoft.jdbc.sqlserver.SQLServerDriver
    conn.url = jdbc:microsoft:sqlserver://localhost:1433;DatabaseName=DB
    conn.username = sa
    conn.password = password
    positionDir = .flume
    batchSize = 100
    tables = YWsj
    tables.YWsj = ST_PPTN_R_FUAN
    tables.YWsj.where = TZ = 'C'
    tables.YWsj.increments = ZID RQ
    tables.YWsj.increments.defaultStart = 2019-01-01 00:00:00
    tables.YWsj.increments.starts.219 = 2019-05-01 00:00:00
    tables.YWsj.increments.includes = 219 210 139 211
    tables.YWsj.columns = ZID RQ DATA TZ
    tables.YWsj.columns.ZID = STCD
    tables.YWsj.columns.RQ = TM
    tables.YWsj.columns.RQ.convert = YEAR(RQ)
    tables.YWsj.columns.DATA = DRP
    tables.YWsj.columns.TZ = TYPE
    ```

    - 双字段自增与单字段自增唯一不同的点就是双字段自增多一个标识字段（如：表的结构是用户id和操作日志时间作为唯一键）。
    - increments.defaultStart 此处表示RQ的默认起始位置，increments.starts.219 表示 219这个标识符的默认起始位置单独设置。
    - increments.includes 表示需要查询的ZID。也可以通过设置 increments.excludes 设置需要排除的标识符，increments.findNew 布尔类型的值表示是否需要动态查询新增的标识符（每次启动服务器都会计算所有的标识符），includes和excludes不能同时指定
    - where 表示除了最基本的查询条件和includes（标识字段）外，需要满足的查询条件，此查询条件需要根据具体数据库进行编写。
    - columns.RQ.convert 表示RQ字段需要进行转换，而转换的方式需要根据具体数据库进行编写。

- sink端

  - 从Channel中获取Event，并将Event转换成数据库中的对象目前存在两种解析方式
    - 将Event的body当做json对象来进行解析
    - 将Event的body当做单个列来进行解析

  - 讲解析出来的对象写入到数据库中存在两种方式
    - 默认使用字段映射
    - 使用指定insert语句

  - 默认字段映射方式，并将Event的body当做json进行解析映射成表的字段，并且在Event的headers中获取table字段获取需要插入的表名

    ```
    type = com.ilivoo.flume.sink.jdbc.JDBCSink
    conn.jdbcUrl = jdbc:mysql://127.0.0.1:3306/test
    conn.maximumPoolSize = 2
    conn.dataSource.user = root
    conn.dataSource.password = root
    dataFormat = bodyJson
    ```

  - 使用插入语句的方式，并将Event的body当做json进行解析作为Event的headers，body字段可以单独使用

    ```
    type = com.ilivoo.flume.sink.jdbc.JDBCSink
    conn.jdbcUrl = jdbc:mysql://127.0.0.1:3306/test
    conn.maximumPoolSize = 2
    conn.dataSource.user = root
    conn.dataSource.password = root
    batchSize = 3
    sql = INSERT INTO $${table} (${myInteger}, ${myString}) VALUES (#{header.myInteger}, #{body})
    dataFormat = bodyJson
    ```

  - 使用字段映射方式，并将Event的headers当做表字段进行映射，并且在Event的headers中获取table字段获取需要插入的表名

    ```
    type = com.ilivoo.flume.sink.jdbc.JDBCSink
    conn.jdbcUrl = jdbc:mysql://127.0.0.1:3306/test
    conn.maximumPoolSize = 2
    conn.dataSource.user = root
    conn.dataSource.password = root
    batchSize = 3
    ```

  - 使用插入语句的方式，并将Event的body当做单字段使用，Event的headers提供其它的字段

    ```
    type = com.ilivoo.flume.sink.jdbc.JDBCSink
    conn.jdbcUrl = jdbc:mysql://127.0.0.1:3306/test
    conn.maximumPoolSize = 2
    conn.dataSource.user = root
    conn.dataSource.password = root
    batchSize = 3
    sql = INSERT INTO $${table} (${myInteger}, ${myString}) VALUES (#{body}, #{header.myString})
    ```

## 开发计划

计划在后期版本中加入功能和优化

- sink不需要重启解决脏数据问题
- sink端表的数据结构发生变化需要重启问题