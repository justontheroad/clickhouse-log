<?php

namespace Clothing\ClickHouseLog;

use \Exception as Exception;

/**
 * 日志转存到ClickHouse
 * 
 * @author laizebiao <laizebiao@globalegrow.com>
 * @version 0.1.0
 */
class LogToClickHouse extends BaseObject
{
    /**
     * 配置要求
     */
    const CONFIG_REQUIRE = [
        'logFieldRequire'  => 'is_array',
        'clickHouseClient' => 'is_object',
        'readLogCallable'  => 'is_callable'
    ];

    /**
     * 日志必要字段
     *
     * @var array
     */
    public $logFieldRequire;
    /**
     * Click House Client
     *
     * @var ClickHouseClient
     */
    public $clickHouseClient;
    /**
     * 读取日志回调方法
     *
     * @var callable
     */
    public $readLogCallable;

    /**
     * 构造函数
     *
     * @param array $config 配置
     */
    public function __construct(array $config)
    {
        parent::__construct($config);

        // 建议必要配置
        foreach (self::CONFIG_REQUIRE as $key => $func) {
            if (!isset($config[$key]) || empty($config[$key]) || (!empty($func) && !$func($config[$key]))) {
                throw new Exception("You must set {$key}");
            }
        }
    }

    /**
     * 传输数据
     *
     * @return void
     */
    public function transfer()
    {
        $logData = call_user_func($this->readLogCallable);
        if (empty($logData)) {
            return;
        }

        $this->clickHouseClient->insert($logData, $this->logFieldRequire);
    }
}
