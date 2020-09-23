<?php

namespace yiiComponent\yiiRocketMq;

use yii\base\InvalidConfigException;
use app\models\Setting;
use MQ\MQClient;
use MQ\Model\TopicMessage;

/**
 * 阿里云RocketMQ
 *
 * Class RocketMQ
 * @package app\components
 */
class RocketMQ extends \app\base\BaseComponent
{
    /**
     * @var string $http 设置HTTP接入域名（此处以公共云生产环境为例）
     */
    public $http;

    /**
     * @var string $accessKey AccessKey阿里云身份验证，在阿里云服务器管理控制台创建
     */
    public $accessKey;

    /**
     * @var string $secretKey SecretKey阿里云身份验证，在阿里云服务器管理控制台创建
     */
    public $secretKey;

    /**
     * @var string $topic Topic
     */
    public $topic;

    /**
     * @var string $groupId 组ID（Consumer ID 或 Product ID）
     */
    public $groupId;

    /**
     * @var string $instanceId Topic所属实例ID，默认实例为空null
     */
    public $instanceId;

    /**
     * @var string [$messageTag = null] 所属的Topic的tag
     */
    public $messageTag = null;

    /**
     * @var object $_client MQClient对象
     */
    private $_client;


    /**
     * 构造方法
     *
     * @param  array [$config = []] 配置属性
     * @return void
     * @throws InvalidConfigException
     */
    public function __construct(array $config = [])
    {
        parent::__construct($config);

        // 阿里云RocketMQ秘钥
        $apiAliyunAK = Setting::findApiAliyunAK();// {"instanceId":"MQ_INST_111533735******","http":"http://11****41607.mqrest.cn-shenzhen.aliyuncs.com","topic":"default","groupId":"GID_default"}
        if (!$apiAliyunAK['accessKey']) {
            throw new InvalidConfigException('accessKey Parameter error.');
        }

        if (!$apiAliyunAK['secretKey']) {
            throw new InvalidConfigException('secretKey Parameter error.');
        }

        $this->accessKey = $apiAliyunAK['accessKey'];
        $this->secretKey = $apiAliyunAK['secretKey'];

        // 阿里云RocketMQ配置
        $apiAliyunRocketeMQ = Setting::findApiAliyunRocketeMQ();
        if (!$apiAliyunRocketeMQ['http']) {
            throw new InvalidConfigException('http Parameter error.');
        }

        if (!$apiAliyunRocketeMQ['topic']) {
            throw new InvalidConfigException('topic Parameter error.');
        }

        if (!$apiAliyunRocketeMQ['instanceId']) {
            throw new InvalidConfigException('instanceId Parameter error.');
        }

        if (!$apiAliyunRocketeMQ['groupId']) {
            throw new InvalidConfigException('groupId Parameter error.');
        }

        $this->http         = $apiAliyunRocketeMQ['http'];
        $this->topic        = $apiAliyunRocketeMQ['topic'];
        $this->groupId      = $apiAliyunRocketeMQ['groupId'];
        $this->instanceId   = $apiAliyunRocketeMQ['instanceId'];
    }

    /**
     * 普通消息推送
     *
     * @param  string $messageBody         消息体
     * @param  string [$messageTag = null] 消息的tag
     * @param  string [$messageId = null]  消息的id
     * @param  string [$messageKey = null] 消息的key
     * @return mixed
     */
    public function push($messageBody, $messageTag = null, $messageId = null, $messageKey = null)
    {
        $topicMessage = $this->_makeTopicMessage($messageBody, $messageTag, $messageId, $messageKey);

        return $result = $this->producer->publishMessage($topicMessage);
    }

    /**
     * 获取消息
     *
     * @param  string [$groupId = null]    消息的groupId
     * @param  string [$messageTag = null] 消息的tag
     * @param  int    [$number = 16]        一次最多消费条数（最多可设置为16条）
     * @param  int    [$waitSeconds = 10]   长轮询时间秒数（最多可设置为30秒）
     * @return mixed
     */
    public function get($messageTag = null, $groupId = null, $number = 16, $waitSeconds = 10)
    {
        $groupId    = $groupId ? $groupId : $this->groupId;
        $messageTag = $messageTag ? $messageTag : $this->messageTag;

        $consumer = $this->getConsumer($groupId, $messageTag);
        $messages = $consumer->consumeMessage(
            $number, // 一次最多消费条数（最多可设置为16条）
            $waitSeconds // 长轮询时间秒数（最多可设置为30秒）
        );
        // 消息出队
        $this->_ackMessage($messages, $groupId, $messageTag);

        return $messages;
    }


    /* ----private---- */

    /**
     * 获取消费者对象
     *
     * @protected
     * @param  string [$groupId = null]    消息的groupId
     * @param  string [$messageTag = null] 消息的tag
     * @return mixed
     */
    protected function getConsumer($groupId, $messageTag)
    {
        return $this->client->getConsumer($this->instanceId, $this->topic, $groupId, $messageTag);
    }

    /**
     * 获取生产对象
     *
     * @protected
     * @return mixed
     */
    protected function getProducer()
    {
        return $this->client->getProducer($this->instanceId, $this->topic);
    }

    /**
     * RocketMQ 获取静态RocketMQ对象
     *
     * @protected
     * @return MQClient|object
     */
    protected function getClient()
    {
        if (!$this->_client) {
            $this->_client = new MQClient($this->http, $this->accessKey, $this->secretKey);
        }

        return $this->_client;
    }

    /**
     * 生成消息对象
     *
     * @private
     * @param  string $messageBody         消息的体
     * @param  string [$messageTag = null] 消息的tag
     * @param  string [$messageId = null]  消息的id
     * @param  string [$messageKey = null] 消息的key
     * @return TopicMessage
     */
    private function _makeTopicMessage($messageBody, $messageTag = null, $messageId = null, $messageKey = null)
    {
        $topicMessage = new TopicMessage($messageBody);

        // 若存在TAG,则要设置TAG
        if ($messageTag) {
            $topicMessage->setMessageTag($messageTag);
        }

        // 若存在messageId,则要设置messageId
        if ($messageId) {
            $topicMessage->setMessageId($messageId);
        }

        // 若存在key,则要设置key
        if ($messageKey) {
            $topicMessage->setMessageKey($messageKey);
        }

        return $topicMessage;
    }

    /**
     * 消息出队
     *
     * @private
     * @param  object $messages 要出队的消息
     * @param  string $groupId 消息的groupId
     * @param  string $messageTag 消息的tag
     * @return void
     */
    private function _ackMessage($messages, $groupId, $messageTag)
    {
        $receiptHandles = [];
        foreach ($messages as $message) {
            $receiptHandles[] = $message->getReceiptHandle();
        }

        try {
            $this->getConsumer($groupId, $messageTag)->ackMessage($receiptHandles);
        } catch (\Exception $e) {
            // TODO 某些消息的句柄可能超时了会导致确认不成功错误记录
        }
    }
}
