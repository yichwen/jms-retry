package io.tao.jms.retry;

import io.tao.jms.retry.message.JmsMQMessage;

public interface MessageSentCallback {
    void messageSent(JmsMQMessage message);
}
