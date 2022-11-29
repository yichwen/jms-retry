package io.tao.jms.retry.message;

import java.util.Map;

public class JmsMQMessageBuilder {

    private final JmsMQMessage message = new JmsMQMessage();

    public JmsMQMessageBuilder addHeader(String header, Object value) {
        message.getHeaders().put(header, value);
        return this;
    }

    public JmsMQMessageBuilder addHeaders(Map<String, Object> headers) {
        message.getHeaders().putAll(headers);
        return this;
    }

    public JmsMQMessageBuilder setPayload(String payload) {
        message.setPayload(payload);
        return this;
    }

    public JmsMQMessage build() {
        return message;
    }

}
