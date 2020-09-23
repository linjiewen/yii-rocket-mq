<?php

use yii\base\InvalidConfigException;

/**
 * Class TestController
 */
class TestController extends \yii\rest\Controller
{
    /**
     * 队列消息分发
     *
     * @return string
     * @throws InvalidConfigException
     */
    public function actionQueue()
    {
        $mq = new \yiiComponent\yiiRocketMq\RocketMQ();
        try {
            $messages = $mq->get();
        } catch (\Exception $e) {
            if ($e instanceof \MQ\Exception\MessageNotExistException) {
                echo "队列信息为空\r\n";
                exit;
            } else {
                throw new InvalidConfigException($e->getMessage());
            }
        }

        // 在当前线程循环消费消息，建议是多开个几个线程并发消费消息
        $yiiPath = Yii::$app->basePath . '/yii';
        foreach ($messages as $v) {
            switch ($v->messageTag) {
                // 登录提醒信息
                case 'loginMsg':
                    // TODO 调用登录提醒信息处理方法
                    break;
                // 用户行为日志
                case 'userSafeLog':
                    $data = json_decode($v->messageBody, true);
                    if ($data['user_id'] && $data['operate'] && $data['remark']) {
                        $userId = $data['user_id'];
                        $operate = $data['operate'];
                        $remark = $data['remark'];
                        exec("php $yiiPath log/user-behavior {$userId} {$operate} \"{$remark}\"", $out, $return);
                        $success = $return ? '执行失败' : '执行成功';
                        echo '用户行为日志（' . $success . '）：' . implode('，', $out) . "\r\n";
                    }

                    break;
                default:
                    break;
            }
        }
    }
}