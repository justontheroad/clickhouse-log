<?php

namespace justontheroad\ClickHouseLog;

use ClickHouseDB\{
    Client,
    Statement,
    Exception\TransportException
};
use \Exception as Exception;

/**
 * Click House 客户端，Http 接口调用 Click House 服务端。
 * 
 * 提供常规的查询，插入，删除方法，不提供更新方法。
 * 
 * 受制于网络等因素，禁止循环调用方法，应尽量使用批量操作，例如批量插入数据。
 * 
 * deprecated, ClickHouseDB\client ping 方法（ClickHouseDB\Transport\Http ping），条件判断异常，建议不要使用。判断点 $request->response()->body() === 'Ok.' . PHP_EOL，服务端实际返回 "Ok. "，匹配失败。
 *
 * ClickHouse 客户端
 * @author laizebiao <laizebiao@globalegrow.com>
 * @version 0.1.0
 */
class ClickHouseClient
{
    /**
     * 默认 端口
     */
    const PORT_DEFAULT = 8123;
    /**
     * 配置要求
     */
    const CONFIG_REQUIRE = [
        'host',
        'port',
        'username',
        'password',
        'database'
    ];

    /**
     * 配置信息
     *
     * @var array
     */
    private $_config;
    /**
     * 客户端
     *
     * @var \ClickHouseDB\Client
     */
    private $_client;
    /**
     * 集群名称
     *
     * @var string
     */
    private $_cluster;
    /**
     * 表名
     *
     * @var string
     */
    private $_table;
    /**
     * 最后一次执行的sql语句
     *
     * @var string
     */
    private $_sql;

    /**
     * 构造函数
     *
     * @param array $config     配置数组
     * @throws Exception        配置信息异常
     */
    public function __construct(array $config)
    {
        foreach (self::CONFIG_REQUIRE as $key) {
            if (!isset($config[$key]) || empty($config[$key])) {
                throw new Exception("You must set {$key}");
            }
        }

        if (!isset($config['port']) || self::PORT_DEFAULT != $config['port']) {
            throw new Exception('You must use port 8123 for HTTP');
        }

        $this->_config = $config;
        $this->init();
    }

    /**
     * 获取一个新的客户端
     *
     * 可调用 ClickHouseDB\Client   提供的所有方法
     * @param array $config         配置信息
     * @return Client
     */
    public function newClient(array $config = []): Client
    {
        empty($config) && $config = $this->_config;
        return new Client($config);
    }

    /**
     * 设置表
     *
     * @param string $table     表名
     * @return void
     */
    public function setTable(string $table): void
    {
        $this->_table = $table;
    }

    /**
     * 获取最后一次执行的sql
     *
     * @return string
     */
    public function getLastSql(): string
    {
        return $this->_sql;
    }

    /**
     * 查询
     *
     * 默认查询一行
     * @param string $whereSql      where条件sql语句，eg. add_time >= :addTime; add_time >= {addTime}; add_time >= '2021-02-01'
     * @param array $bindings       绑定条件，eg. ['addTime' = '2021-02-01']
     * @param string|array $columns 列，eg. '`id`, `add_time`'; ['id', 'add_time']
     * @param array $order          排序['order' => $order, 'sort' => 'DESC']
     * @return array                数据
     * @throws TransportException   传输异常
     * @throws Exception            异常
     */
    public function select(string $whereSql = '', array $bindings = [], $columns = '*', array $order = [], int $offset = 0, int $limit = 1): array
    {
        $sql = empty($whereSql) ? '' : "WHERE $whereSql";
        $sql = "SELECT {columns} FROM {table} {$sql}";

        if (!empty($order)) {
            $sql      =  "{$sql} ORDER BY {order} {sort}";
            $bindings = array_merge($bindings, $order);
        }

        $sql      = " {$sql} LIMIT {limit} OFFSET {offset}";
        $bindings = array_merge(['columns' => Condition::columns($columns), 'table' => $this->_table], $bindings, ['limit' => $limit, 'offset' => $offset]);
        $stat     = $this->_client->select($sql, $bindings);

        $this->_sql = $stat->sql();

        return 1 < $limit ? $stat->rows() : $stat->fetchOne();
    }

    /**
     * 查询总数
     *
     * @param string $whereSql      where条件sql语句，eg. add_time >= :addTime; add_time >= {addTime}; add_time >= '2021-02-01'
     * @param array $bindings       绑定条件，eg. ['addTime' = '2021-02-01']
     * @param string $key           count($key)，count空键值效率更高，建议使用默认值
     * @return integer              总数
     * @throws TransportException   传输异常
     * @throws Exception            异常
     */
    public function selectCount(string $whereSql = '', array $bindings = [], string $key = ''): int
    {
        $sql      = empty($whereSql) ? '' : "WHERE $whereSql";
        $sql      = empty($columns) ? "SELECT COUNT() AS `count` FROM {table} {$sql}" : "SELECT COUNT($key) AS `count` FROM {table} {$sql}";
        $bindings = array_merge(['table' => $this->_table], $bindings);
        $stat     = $this->_client->select($sql, $bindings);

        $this->_sql = $stat->sql();

        return $stat->fetchOne('count');
    }

    /**
     * 异步查询
     *
     * @param array $sqlMap         sql列表，eg. [$key1 => $sql1, $key2 => $sql2...]
     * @return array                Statement 数组，执行失败的不会返回，可以使用isset()判断
     * @throws TransportException   传输异常
     */
    public function asyncSelect(array $sqlMap)
    {
        $processes = [];
        $stats     = [];

        foreach ($sqlMap as $key => $sql) {
            $processes[$key] = $this->_client->selectAsync($sql);
        }

        $this->_client->executeAsync();
        foreach (array_keys($sqlMap) as $key) {
            try {
                $stat = $processes[$key]->fetchOne();
                $stats[$key] = $stat;
            } catch (Exception $e) {
            }
        }

        return $stats;
    }

    /**
     * 插入
     *
     * @param array $values         插入值，必须与$columns 一一对应，eg. [['id' => 1, 'add_time' => '2021-02-06'], ['id' => 2, 'add_time' => '2021-02-06']]
     * @param array $columns        列，eg. ['id', 'add_time']  
     * @return Statement            声明信息
     * @throws TransportException   传输异常
     */
    public function insert(array $values, array $columns)
    {
        $this->_client->settings()->readonly(false);
        $stat = $this->_client->insert($this->_table, $values, $columns);

        $this->_sql = $stat->sql();

        return $stat;
    }

    /**
     * 删除
     *
     * @param string $whereSql      where条件sql语句，eg. add_time >= :addTime; add_time >= {addTime}; add_time >= '2021-02-01'
     * @param array $bindings       绑定条件，eg. ['addTime' = '2021-02-01']
     * @param string $partition     分区key
     * @throws TransportException   传输异常
     * @throws Exception            异常
     */
    public function delete(string $whereSql, array $bindings = [], string $partition = '')
    {
        $this->_client->settings()->readonly(false);

        $sql             = '';
        $partitionParams = '';
        $clusterParams   = '';

        if (!empty($partition)) {
            $partitionParams = 'IN PARTITION {partition}';
            $bindings        = array_merge(['partition' => $partition], $bindings);
        }

        if (!empty($this->_cluster)) {
            $clusterParams = 'ON CLUSTER {cluster}';
            $bindings      = array_merge(['cluster' => $this->_cluster], $bindings);
        }

        $sql      = "ALTER TABLE {table} {$clusterParams} DELETE {$partitionParams} WHERE $whereSql";
        $bindings = array_merge(['table' => $this->_table], $bindings);

        $stat = $this->_client->write($sql, $bindings);

        $this->_sql = $stat->sql();

        return $stat;
    }

    /**
     * 初始化
     *
     * @return void
     */
    private function init()
    {
        !isset($this->_config['timeout']) && $this->_config['timeout'] = 10;
        !isset($this->_config['connect_timeout']) && $this->_config['connect_timeout'] = 5;
        $this->_cluster = $this->_config['cluster'] ?? '';

        $this->getClient();

        $this->_client->database($this->_config['database']);
        $this->_client->setTimeout($this->_config['timeout']);
        $this->_client->setConnectTimeOut($this->_config['connect_timeout']);
    }

    /**
     * 获取客户端
     *
     * @return Client
     */
    private function getClient(): Client
    {
        if (!($this->_client instanceof Client)) {
            $this->_client = new Client($this->_config);
        }

        return $this->_client;
    }
}

class Condition
{
    public static function columns($columns)
    {
        if (is_array($columns)) {
            $columns = array_map(function ($item) {
                return "`{$item}`";
            }, $columns);
            return implode(',', $columns);
        }

        return $columns;
    }
}
