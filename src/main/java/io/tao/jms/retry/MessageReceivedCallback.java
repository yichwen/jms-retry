package io.tao.jms.retry;

import io.tao.jms.retry.message.JmsMQMessage;

public interface MessageReceivedCallback {
    void messageReceived(JmsMQMessage message);
}
