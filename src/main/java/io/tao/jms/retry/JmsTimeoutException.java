package io.tao.jms.retry;

import org.springframework.jms.JmsException;

public class JmsTimeoutException extends JmsException {
    public JmsTimeoutException(String msg) {
        super(msg);
    }
}
