package io.tao.jms.retry.message;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

@Data
@Slf4j
public class JmsMQMessage {

    public static final String JMS_MESSAGE_ID = "JMSMessageID";
    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_TYPE = "JMSType";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_DELIVERY_TIME = "JMSDeliveryTime";
    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_REDELIVERED = "JMSRedelivered";
    public static final String JMS_REPLY_TO = "JMSReplyTo";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";

    private Map<String, Object> headers = new HashMap<>();
    private String payload;

    public Object getHeader(String id) {
        return headers.get(id);
    }

    public void setHeader(String id, Object value) {
        headers.put(id, value);
    }

    public static JmsMQMessageBuilder builder() {
        return new JmsMQMessageBuilder();
    }

    private static boolean isAllowedProperty(String key) {
        return !key.equals(JMS_MESSAGE_ID) && !key.equals(JMS_DELIVERY_MODE) && !key.equals(JMS_TYPE) &&
                !key.equals(JMS_DESTINATION) && !key.equals(JMS_DELIVERY_TIME) && !key.equals(JMS_PRIORITY) &&
                !key.equals(JMS_TIMESTAMP) && !key.equals(JMS_REDELIVERED) && !key.equals(JMS_EXPIRATION);
    }

    /**
     * populate headers from JmsMQMessage to javax.jms.Message
     * @param message a JmsMQMessage
     * @param jmsMessage a javax.jms.Message
     */
    public static void populateJmsHeaders(JmsMQMessage message, Message jmsMessage) {

        for (String headerName: message.getHeaders().keySet()) {

            if (isAllowedProperty(headerName)) {

                if (headerName.equals(JMS_CORRELATION_ID)) {

                    Object value = message.getHeader(headerName);
                    if (value instanceof String) {
                        try {
                            jmsMessage.setJMSCorrelationID((String) value);
                        } catch (JMSException ex) {
                            log.debug("failed to set JMSCorrelationID property - skipping", ex);
                        }
                    } else {
                        log.debug("failed to set JMSCorrelationID property, invalid value {}, only support String type - skipping", value);
                    }

                } else if (headerName.equals(JMS_REPLY_TO)) {

                    Object value = message.getHeader(headerName);
                    if (value instanceof Destination) {
                        try {
                            jmsMessage.setJMSReplyTo((Destination) message.getHeader(headerName));
                        } catch (JMSException ex) {
                            log.debug("failed to set JMSReplyTo property - skipping", ex);
                        }
                    } else {
                        log.debug("failed to set JMSReplyTo property, invalid value {}, only support Destination type - skipping", value);
                    }

                } else {
                    Object value = message.getHeader(headerName);
                    if (value != null) {
                        try {
                            jmsMessage.setObjectProperty(headerName, value);
                        } catch (JMSException ex) {
                            log.debug("failed to set {} property - skipping", headerName);
                        }
                    }
                }

            } else {
                log.debug("property {} is not allowed - skipping", headerName);
            }
        }
    }

    /**
     * to convert javax.jms.Message to JmsMQMessage
     * @param message a javax.jms.Message
     * @return a JmsMQMessage
     */
    public static JmsMQMessage fromJmsMessage(Message message) {

        JmsMQMessageBuilder builder = JmsMQMessage.builder();

        try {
            String value = message.getJMSCorrelationID();
            if (value != null) {
                builder.addHeader(JMS_CORRELATION_ID, value);
            }
        } catch (JMSException ex) {
            log.debug("failed to read JMSCorrelationID property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_DELIVERY_MODE, message.getJMSDeliveryMode());
        } catch (JMSException ex) {
            log.debug("failed to read JMSDeliveryMode property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_DELIVERY_TIME, message.getJMSDeliveryTime());
        } catch (JMSException ex) {
            log.debug("failed to read JMSDeliveryTime property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_DESTINATION, message.getJMSDestination());
        } catch (JMSException ex) {
            log.debug("failed to read JMSDestination property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_EXPIRATION, message.getJMSExpiration());
        } catch (JMSException ex) {
            log.debug("failed to read JMSExpiration property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_MESSAGE_ID, message.getJMSMessageID());
        } catch (JMSException ex) {
            log.debug("failed to read JMSMessageID property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_PRIORITY, message.getJMSPriority());
        } catch (JMSException ex) {
            log.debug("failed to read JMSPriority property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_REDELIVERED, message.getJMSRedelivered());
        } catch (JMSException ex) {
            log.debug("failed to read JMSRedelivered property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_REPLY_TO, message.getJMSReplyTo());
        } catch (JMSException ex) {
            log.debug("failed to read JMSReplyTo property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_TIMESTAMP, message.getJMSTimestamp());
        } catch (JMSException ex) {
            log.debug("failed to read JMSTimestamp property - skipping", ex);
        }

        try {
            builder.addHeader(JMS_TYPE, message.getJMSType());
        } catch (JMSException ex) {
            log.debug("failed to read JmsType property - skipping", ex);
        }

        try {
            Enumeration<?> propertyNames = message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement().toString();
                Object propertyValue = message.getObjectProperty(propertyName);
                builder.addHeader(propertyName, propertyValue);
            }
        } catch (JMSException ex) {
            log.debug("failed to read property names - skipping", ex);
        }
        
        try {
            if (message instanceof TextMessage) {
                builder.setPayload(((TextMessage) message).getText());
            } else {
                log.debug("message is {}, only support TextMessage - skipping", message.getClass().getSimpleName());
            }
        } catch (JMSException ex) {
            log.debug("failed to read text - skipping", ex);
        }

        return builder.build();
    }

}
