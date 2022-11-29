package io.tao.jms.retry;

import org.springframework.jms.JmsException;
import org.springframework.jms.core.JmsTemplate;

public interface RequestOnlyProcessor {
    void processRequestOnly(JmsTemplate jmsTemplate) throws JmsException;
}
