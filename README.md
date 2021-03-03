# 服装 ClickHouse 日志
## composer 安装本项目
1. 命令行执行安装
    ```
    composer require justontheroad/clickhouse-log
    ```

## ClickHouse Client 使用示例

### ClickHouse 表结构
```
-- 本地表
CREATE TABLE IF NOT EXISTS `log_data`.`base_log` \
( \
    `id` UInt64 COMMENT 'id', \
    `level` UInt32 COMMENT '日志等级', \
    `request_host` String COMMENT '请求host', \
    `request_uri` String COMMENT '请求uri', \
    `message` String COMMENT '日志消息', \
    `content` String COMMENT '日志内容', \
    `add_date` Date COMMENT '日期', \
    `add_time` DateTime COMMENT '时间', \
    `server_ip` String COMMENT '服务器ip' \
) \
ENGINE = MergeTree\
PARTITION BY toYYYYMM(add_time)\
PRIMARY KEY add_time\
ORDER BY add_time\
SETTINGS index_granularity = 8192;

-- 分布式表
CREATE TABLE  IF NOT EXISTS `clothing_log`.`distri_base_log` on cluster ck_cluster_distributed \
( \
    `id` UUID DEFAULT generateUUIDv4() COMMENT '唯一id', \
    `level` UInt32 COMMENT '日志等级', \
    `request_host` String COMMENT '请求host',\
    `request_uri` String COMMENT '请求uri', \
    `message` String COMMENT '日志消息', \
    `content` String COMMENT '日志内容', \
    `add_date` Date COMMENT '日期', \
    `add_time` DateTime COMMENT '时间', \
    `server_ip` String COMMENT '服务器ip' \
) \
ENGINE = Distributed(ck_cluster_distributed , clothing_log, base_log, rand());
```

### 配置信息
```
// 本地测试环境
$clickHouseConfig = [
    'db'  => [
        'host'            => '192.168.143.131', // you hot name
        'port'            => '8123',
        'username'        => 'developer',
        'password'        => '9YzQuhLV',
        'database'        => 'log_data',
        'timeout'         => 10, // request time out
        'connect_timeout' => 5, // connect time out
        'table'           => 'base_log',
        'storage_table'   => 'base_log'
    ],
    'log' => [
        'logFile'    => SYS_PATH . 'runtime/logs/elog/customize/base.json',
        'offsetFile' => SYS_PATH . 'runtime/logs/elog/customize/offset.json',
        'readRows'   => 100
    ],
    'jobsLog' => [
        'logFile'    => SYS_PATH . 'runtime/logs/jobs/customize/base.json',
        'offsetFile' => SYS_PATH . 'runtime/logs/jobs/customize/offset.json',
        'readRows'   => 100
    ]
]

// 线上集群环境
$clickHouseConfig = [
    'db'  => [
        'host'            => '10.95.240.40', // any node name in cluster
        'port'            => '8123',
        'username'        => 'clothinglog_user',
        'password'        => 'uiFDD8s3F9KTjend',
        'database'        => 'clothing_log',
        'timeout'         => 10, // request time out
        'connect_timeout' => 5, // connect time out
        'cluster'         => 'ck_cluster_distributed',
        'table'           => 'distri_base_log',
        'storage_table'   => 'base_log'
    ],
    'log' => [
        'logFile'    => SYS_PATH . 'runtime/logs/elog/customize/base.json',
        'offsetFile' => SYS_PATH . 'runtime/logs/elog/customize/offset.json',
        'readRows'   => 100
    ],
    'jobsLog' => [
        'logFile'    => SYS_PATH . 'runtime/logs/jobs/customize/base.json',
        'offsetFile' => SYS_PATH . 'runtime/logs/jobs/customize/offset.json',
        'readRows'   => 100
    ]
]
```

### 引入
```
use justontheroad\ClickHouseLog\ClickHouseClient;
```

### 添加数据
```
$client = new ClickHouseClient($clickHouseConfig['db']);
$client->setTable('base_log');
$stat   = $client->insert(
    [
        [400, 'www.price.com.local.php7.egomsl.com', '/admin/coupon-system/list', 'yii system error', 'SQLSTATE[42S22]: Column not found: 1054 Unknown column', '2021-02-08', '2021-02-08 10:10:46', '127.0.0.1'],
        [400, 'www.price.com.local.php7.egomsl.com', '/admin/coupon-system/list', 'yii system error', 'SQLSTATE[42S22]: Column not found: 1054 Unknown column', '2021-02-09', '2021-02-09 10:10:46', '127.0.0.1']
    ],
    ['level', 'request_host', 'request_uri', 'message', 'context', 'add_date', 'add_time']
);
```

### 查询数据
```
$client = new ClickHouseClient($clickHouseConfig['db']);
$client->setTable('base_log');
// 同步
$result = $client->select('`add_date` = \'{addDate}\'', ['addDate' => '2021-02-01']);
// 异步
$sqlMap = [];
$table  = 'base_log';
for ($f = 1; $f < 4; $f++) {
    $addDate    = '2021-02-0' . $f;
    $sqlMap[$f] = "SELECT `id` FROM {$table} WHERE `add_date` = '{$addDate}' LIMIT 0, 1";
}

$stats = $client->asyncSelect($sqlMap);
```

### 删除数据
$client = new ClickHouseClient($clickHouseConfig['db']);
$client->setTable('base_log');
```
$stat   = $client->delete("`add_date` < '{add_date}'", ['add_date' => $addDate]);
```

### 推送 log 到 ClickHouse
```
$dbConfig        = $clickHouseConfig['db'];
$logConfig       = $clickHouseConfig['log'];
$logFieldRequire = [
    'level',
    'request_host',
    'request_uri',
    'message',
    'content',
    'add_date',
    'add_time',
    'server_ip'
];
$res             = '';
$serverIp        = @file_get_contents(SYS_PATH . 'runtime/server_ip.txt');

try {
    $client     = new ClickHouseClient($dbConfig);
    $client->setTable($dbConfig['table']);
    $elogReader = new ElogReader(new LogManager($logConfig));
    !empty($serverIp) && $elogReader->serverIp = $serverIp;
    $config     = [
        'logFieldRequire'  => $logFieldRequire,
        'clickHouseClient' => $client,
        'readLogCallable'  => [$elogReader, 'read']
    ];
    $logTo      = new LogToClickHouse($config);
    $logTo->transfer();
    $res = $this->response(200, []);
} catch (Exception $e) {
    $res = $this->response(500, ['exception' => $e->getMessage()]);
}
```