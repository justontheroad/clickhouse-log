<?php


namespace Clothing\ClickHouseLog;

use \SplFileObject as SplFileObject;
use \Exception as Exception;

/**
 * 日志管理
 * 
 * v0.2.0 更新内容：当文件发生改变（产生新文件），继续校验最后一个历史文件。如果历史文件不等于当前“日志文件”，且当历史文件大小大于当前记录，文件句柄指向历史文件。
 * 
 * @author laizebiao <laizebiao@globalegrow.com>
 * @version 0.2.0
 */
class LogManager extends BaseObject
{
    /**
     * 配置要求
     */
    const CONFIG_REQUIRE = [
        'logFile',
        'offsetFile'
    ];

    /**
     * 日志文件名
     *
     * 使用绝对路径 eg. /var/www/html/base/runtime/logs/elog/customize/price.json
     * @var string 日志文件名
     */
    public $logFile;
    /**
     * 偏移量文件
     *
     * 使用绝对路径 eg. /var/www/html/base/runtime/logs/elog/customize/offset.json
     * @var string 偏移量文件
     */
    public $offsetFile;
    /**
     * 分割规则
     * 目前仅支持换行符，默认：1
     * 
     * 1：换行符
     * @var integer
     */
    public $splitRules = 1;
    /**
     * 读取的行数
     * 分割规则为换行符时，必填
     *
     * @var integer 读取的行数，默认1000行
     */
    public $readRows = 1000;
    /**
     * 文件句柄
     *
     * @var SplFileObject
     */
    private $_fileHandler = null;

    /**
     * 构造函数
     *
     * @param array $config 配置
     */
    public function __construct(array $config)
    {
        parent::__construct($config);

        // 建议必要配置
        foreach (self::CONFIG_REQUIRE as $key) {
            if (!isset($config[$key]) || empty($config[$key])) {
                throw new Exception("You must set {$key}");
            }
        }
    }

    /**
     * 逐行读取文件
     *
     * @return array    内容buffer数组
     */
    public function readFileLineByLine()
    {
        $contents = [];
        $this->fileHandler();
        // $handle   = $this->fileHandler(); // getStartLine 可能会修改当前文件 handle，所以只能用类的成员变量代替，不能使用局部变量 —— 可能指向上一个文件

        if ($this->_fileHandler) {
            list($startLine, $size) = $this->getStartLine();
            $this->_fileHandler->seek($startLine); // 转到$startLine行，seek方法从0开始计数

            for ($i = 0; $i < $this->readRows && !$this->_fileHandler->eof(); ++$i) {
                $content = trim($this->_fileHandler->current());
                if (empty($content)) {
                    continue;
                }

                $contents[] = $content;
                $this->_fileHandler->next(); // 循环下一行
                $startLine++;
            }
            $this->setStartLine($startLine, $size);
        }

        return $contents;
    }

    /**
     * 清理偏移量文件
     *
     * @return void
     */
    public function cleanOffsetFile()
    {
        @file_put_contents($this->offsetFile, '');
    }

    /**
     * 日志文件列表
     *
     * 只查询一级目录下的文件
     * @param  bool   $isFullName   是否全名，默认获取全名
     * @param  bool   $asc          是否升序，默认文件名降序
     * @return array                文件列表
     * @after v.0.1.0
     */
    public function logFiles(bool $isFullName = true, bool $asc = false): array
    {
        $path = substr($this->logFile, 0, strrpos($this->logFile, '/') + 1);
        if (!is_dir($path)) {
            return [];
        }

        $files = [];

        if ($handle = opendir($path)) {
            try {
                while (false !== ($file = readdir($handle))) {
                    if ('.' != $file && '..' != $file && is_file($path . $file)) {
                        if ($isFullName) {
                            $files[] = $path . $file;
                        } else {
                            $files[] = $file;
                        }
                    }
                }
            } catch (Exception $e) {
                $files = [];
            } finally {
                closedir($handle);
            }
        }

        if ($asc) {
            sort($files);
        } else {
            rsort($files);
        }

        return $files;
    }

    /**
     * 获取最后一个历史日志文件
     *
     * @param array $logFiles       日志文件列表
     * @return string               日志文件全名
     */
    public function lastLogFile(array $logFiles): string
    {
        $logFile = substr($this->logFile, strrpos($this->logFile, '/') + 1);
        $count   = count($logFiles);
        $file    = '';
        rsort($logFiles); // 根据文件名降序

        for ($i = 0; $i < $count; $i++) {
            if (stripos($logFiles[$i], $logFile)) {
                $file = $logFiles[$i];
                break;
            }
        }

        return !empty($file) ? $file : $this->logFile;
    }

    /**
     * 判断日志文件已经改变
     *
     * 通过文件size判断，日志文件size递增，当size缩减时，代表文件被重新创建
     * @param integer &$size        文件大小，引用
     * @return boolean              文件是否改变
     */
    private function hasChanged(int &$size)
    {
        $handle = $this->fileHandler();

        if (!$handle) {
            return false;
        }

        $fstat = $handle->fstat();
        $fSize = $fstat['size'] ?? 0;

        if ($fSize < $size) {
            $size = $fSize;
            return true;
        }

        $size = $fSize;
        return false;
    }

    /**
     * 获取起始行
     *
     * @version 0.2.0               当文件发生改变（产生新文件），继续校验最后一个历史文件。如果历史文件不等于当前“日志文件”，且当历史文件大小大于当前记录，文件句柄指向历史文件。
     * @return array                起始行数$startLine，文件size
     */
    private function getStartLine(): array
    {
        $content = @file_get_contents($this->offsetFile);
        $content = json_decode($content, true);
        $default = [
            'startLine' => 0,
            'size'      => 0
        ];

        empty($content) && $content = $default;
        $originSize = $content['size'];

        // 当文件发生改变（产生新文件）
        // 继续校验最后一个历史文件。如果历史文件不等于当前“日志文件”，且当历史文件大小大于当前记录，文件句柄指向历史文件。
        if ($this->hasChanged($content['size'])) {
            $logFiles    = $this->logFiles(true, false);
            $lastLogFile = $this->lastLogFile($logFiles);

            if ($lastLogFile != $this->logFile) {
                $handle = $this->newFileHandler($lastLogFile);
                $fstat  = $handle->fstat();
                $fSize  = $fstat['size'] ?? 0;

                if ($fSize > $originSize) {
                    // 当前文件句柄指向历史文件
                    $this->setFileHandler($handle);
                    return [intval($content['startLine']), intval($fSize)];
                }
            }

            $content['startLine'] = 0;
        }

        return [intval($content['startLine']), intval($content['size'])];
    }

    /**
     * 设置起始行
     *
     * @param integer $startLine    开始行
     * @param integer $size         文件大小
     * @return void
     */
    private function setStartLine(int $startLine, int $size): void
    {
        $data = [
            'startLine' => $startLine,
            'size'      => $size
        ];
        @file_put_contents($this->offsetFile, json_encode($data));
    }

    /**
     * 获取文件句柄
     *
     * @return SplFileObject
     */
    private function fileHandler(): SplFileObject
    {
        if (!($this->_fileHandler instanceof SplFileObject)) {
            $this->_fileHandler = new SplFileObject($this->logFile, 'rb');
        }

        return $this->_fileHandler;
    }

    /**
     * 设置文件句柄
     *
     * @param SplFileObject $handler    文件句柄
     * @return void
     */
    private function setFileHandler(SplFileObject $handler): void
    {
        $this->_fileHandler = $handler;
    }

    /**
     * 获取新的文件句柄
     *
     * @return SplFileObject
     */
    private function newFileHandler(string $file): SplFileObject
    {
        return new SplFileObject($file, 'rb');
    }
}
