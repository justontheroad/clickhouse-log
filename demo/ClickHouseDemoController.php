<?php

namespace app\controllers;

use justontheroad\ClickHouseLog\{
    ClickHouseClient,
    LogToClickHouse,
    LogManager,
    ElogReader
};
use \Exception as Exception;
use Yii;

set_time_limit(60);

class ClickHouseDemoController extends \yii\web\Controller
{
    const DB_CONFIG = [
        'host'            => '192.168.143.131', // you hot name
        'port'            => '8123',
        'username'        => 'developer',
        'password'        => '9YzQuhLV',
        'database'        => 'log_data',
        'timeout'         => 10, // request time out
        'connect_timeout' => 5, // connect time out
        'table'           => 'base_log',
        'storage_table'   => 'base_log'
    ];

    const LOG_CONFIG = [
        'logFile'    => SYS_PATH . 'runtime/logs/test.json',
        'offsetFile' => SYS_PATH . 'runtime/logs/offset.json',
        'readRows'   => 100
    ];

    private $_client;

    public function actionList()
    {
        $res = '';
        list($whereSql, $bindings, $order, $offset, $pageSize) = $this->params();

        try {
            $client = $this->getClient();
            $list   = $client->select($whereSql, $bindings, '*', $order, $offset, $pageSize);
            $res    = $this->response(200, ['list' => $list, 'sql' => $client->getLastSql()]);
        } catch (Exception $e) {
            $res = $this->response(500, ['exception' => $e->getMessage()]);
        }

        return $res;
    }

    public function actionCount()
    {
        $res = '';
        list($whereSql, $bindings, $order, $offset, $pageSize) = $this->params();

        try {
            $client = $this->getClient();
            $count  = $client->selectCount($whereSql, $bindings);
            $res    = $this->response(200, ['count' => $count, 'sql' => $client->getLastSql()]);
        } catch (Exception $e) {
            $res = $this->response(500, ['exception' => $e->getMessage()]);
        }

        return $res;
    }

    public function actionDeletePart()
    {
        $params  = Yii::$app->request->get();
        $spacing = isset($params['spacing']) ? intval($params['spacing']) : 5184000; // 默认两个月前，3600 * 24 * 60
        $time    = Yii::$app->bjTime->time() - $spacing;
        $part    = Yii::$app->bjTime->date('Ym', $time);
        $addDate = Yii::$app->bjTime->date('Y-m-d', Yii::$app->bjTime->strtotime($part . '01'));
        $res     = '';

        try {
            $client = $this->getClient();
            $client->setTable(self::DB_CONFIG['storage_table']);
            $stat   = $client->delete("`add_date` >= '{add_date}'", ['add_date' => $addDate], $part);
            $res    = $this->response(200, ['state' => $stat, 'sql' => $stat->sql()]);
        } catch (Exception $e) {
            $res = $this->response(500, ['exception' => $e->getMessage()]);
        }

        return $res;
    }

    public function actionDelete()
    {
        $params  = Yii::$app->request->get();
        $spacing = isset($params['spacing']) ? intval($params['spacing']) : 5184000; // 默认两个月前，3600 * 24 * 60
        $time    = Yii::$app->bjTime->time() - $spacing;
        $addDate = Yii::$app->bjTime->date('Y-m-d', $time);
        $res     = '';

        try {
            $client = $this->getClient();
            $client->setTable(self::DB_CONFIG['storage_table']);
            $stat   = $client->delete("`add_date` < '{add_date}'", ['add_date' => $addDate]);
            $res    = $this->response(200, ['state' => $stat, 'sql' => $stat->sql()]);
        } catch (Exception $e) {
            $res = $this->response(500, ['exception' => $e->getMessage()]);
        }

        return $res;
    }

    public function actionLogToClickHouse()
    {
        $params          = Yii::$app->request->get();
        $readRows        = $params['readRows'] ?? null;
        $dbConfig        = self::DB_CONFIG;
        $logConfig       = self::LOG_CONFIG;
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

        if (is_numeric($readRows)) {
            $logConfig['readRows'] = intval($readRows);
        }

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

        return $res;
    }

    public function actionCleanOffsetFile()
    {
        $logConfig = self::LOG_CONFIG;
        $manager   = new LogManager($logConfig);
        $manager->cleanOffsetFile();

        return $this->response(200, []);
    }

    private function params()
    {
        $params    = Yii::$app->request->get();
        $page      = isset($params['page']) ? intval($params['page']) : 1;
        $pageSize  = isset($params['pageSize']) ? intval($params['pageSize']) : 100;
        $addDate   = isset($params['addDate']) ? trim($params['addDate']) : '';
        $startDate = isset($params['startDate']) ? trim($params['startDate']) : '';
        $endDate   = isset($params['endDate']) ? trim($params['endDate']) : '';
        $level     = isset($params['level']) ? intval($params['level']) : null;
        $content   = isset($params['content']) ? trim($params['content']) : '';
        $order     = isset($params['order']) ? trim($params['order']) : 'add_time';
        $offset    = (max($page, 1) - 1) * max($pageSize, 1);
        $whereSql  = [];
        $bindings  = [];
        $order     = ['order' => $order, 'sort' => 'DESC'];

        if (!empty($addDate)) {
            $whereSql[] = '`add_date` = \'{addDate}\'';
            $bindings['addDate'] = $addDate;
        } else if (!empty($startDate) && !empty($endDate)) {
            $whereSql[] = '`add_date` >= \'{startDate}\' AND `add_date` <= \'{endDate}\'';
            $bindings['startDate'] = $startDate;
            $bindings['endDate'] = $endDate;
        }

        if (is_int($level)) {
            $whereSql[] = '`level` = {level}';
            $bindings['level'] = $level;
        }

        if (!empty($content)) {
            $whereSql[] = '`content` LIKE \'%{content}%\'';
            $bindings['context'] = $context;
        }

        $whereSql = implode(' AND', $whereSql);

        return [$whereSql, $bindings, $order, $offset, $pageSize];
    }

    private function getClient()
    {
        if (!($this->_client instanceof ClickHouseClient)) {
            $this->_client = new ClickHouseClient(self::DB_CONFIG);
        }

        $this->_client->setTable('base_log');
        return $this->_client;
    }

    private function response($code, $data)
    {
        return json_encode(['code' => $code, 'data' => $data]);
    }
}
