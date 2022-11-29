package io.tao.jms.retry;

import io.tao.jms.retry.message.JmsMQMessage;
import org.springframework.jms.JmsException;
import org.springframework.jms.core.JmsTemplate;

public interface RequestReplyProcessor {
    JmsMQMessage processRequestReply(JmsTemplate jmsTemplate) throws JmsException;
}
