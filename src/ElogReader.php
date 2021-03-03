<?php

namespace Clothing\ClickHouseLog;

/**
 * elog 读取器
 * 
 * @author laizebiao <laizebiao@globalegrow.com>
 * @version 0.1.0
 */
class ElogReader extends BaseObject
{
    /**
     * 服务器ip
     *
     * @var string
     */
    public $serverIp = '';

    /**
     * log 管理器
     *
     * @var LogManager
     */
    private $_logManager = null;

    public function __construct(LogManager $logManager)
    {
        $this->_logManager = $logManager;
    }

    /**
     * 读取数据
     *
     * @return array    数据 [[$level, $http_host, $request_uri, $message, $context, $add_date, $add_time, $server_ip]]
     */
    public function read(): array
    {
        $contents = $this->_logManager->readFileLineByLine();
        $data     = [];

        foreach ($contents as $content) {
            $content = json_decode($content, true);
            if (empty($content)) {
                continue;
            }

            $dateTime = new \DateTime($content['datetime']['date'], new \DateTimeZone($content['datetime']['timezone']));
            $dateTime->setTimezone(new \DateTimeZone('Asia/Shanghai')); // 调整为北京时间
            $data[] = [
                $content['level'],                      // level
                $content['extra']['http_host'] ?? '',   // http_host
                $content['extra']['request_uri'] ?? '', // request_uri
                $content['message'],                    // message
                json_encode($content['context']),       // context
                $dateTime->format('Y-m-d'),             // add_date
                $dateTime->format('Y-m-d H:i:s'),       // add_time
                $this->serverIp                         // server_ip
            ];
        }

        return $data;
    }
}
