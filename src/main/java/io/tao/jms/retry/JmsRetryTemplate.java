package io.tao.jms.retry;

import io.tao.jms.retry.message.JmsMQMessage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jms.JmsException;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.util.Assert;

import javax.jms.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class JmsRetryTemplate implements InitializingBean {

    private static final String TAO_RETRY_COUNT = "TAO_RETRY_COUNT";

    @Getter
    @Setter
    private JmsTemplate jmsTemplate;
    @Setter
    private MessageConverter messageConverter;
    @Getter @Setter
    private String destination;
    @Getter @Setter
    private int maxAttempts = 1;
    @Getter
    private final Set<Class<? extends Throwable>> retryableExceptions = new HashSet<>();
    @Getter @Setter
    private MessageSentCallback messageSentCallback;
    @Getter @Setter
    private MessageReceivedCallback messageReceivedCallback;
    @Getter @Setter
    private MessageRetryProcessor messageRetryProcessor;

    public JmsRetryTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    public MessageConverter getMessageConverter() {
        if (messageConverter == null) {
            if (this.jmsTemplate != null) {
                messageConverter = this.jmsTemplate.getMessageConverter();
            }
        }
        return messageConverter;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.jmsTemplate == null) {
            throw new IllegalArgumentException("property 'jmsTemplate' is required");
        }
    }

    public void addRetryableException(Class<? extends Throwable> tClazz) {
        this.retryableExceptions.add(tClazz);
    }

    public boolean isRetryableException(Throwable t) {
        Throwable t1 = t;
        while (t1 != null) {
            if (this.retryableExceptions.contains(t.getClass())) {
                return true;
            }
            t1 = t1.getCause();
        }
        return false;
    }

    protected void doSend(JmsMQMessage message, RequestOnlyProcessor processor) {

        final AtomicInteger retryCount = new AtomicInteger(0);

        while (true) {

            if (retryCount.get() > 0 && getMessageRetryProcessor() != null) {
                getMessageRetryProcessor().processRetryMessage(message);
                message.setHeader(TAO_RETRY_COUNT, retryCount.get());
            }

            try {
                processor.processRequestOnly(this.jmsTemplate);
                return;
            } catch (JmsException ex) {
                if (isRetryableException(ex) && retryCount.incrementAndGet() <= getMaxAttempts()) {
                    // backoff required ? may apply spring RetryTemplate
                } else {
                    throw ex;
                }
            }
        }
    }

    protected JmsMQMessage doSendAndReceive(JmsMQMessage message, RequestReplyProcessor processor) {

            final AtomicInteger retryCount = new AtomicInteger(0);

            while (true) {

                if (retryCount.get() > 0 && getMessageRetryProcessor() != null) {
                    getMessageRetryProcessor().processRetryMessage(message);
                    message.setHeader(TAO_RETRY_COUNT, retryCount.get());
                }

                try {
                    return processor.processRequestReply(this.jmsTemplate);
                } catch (JmsException ex) {
                    if (isRetryableException(ex) && retryCount.incrementAndGet() <= getMaxAttempts()) {
                        // backoff required ? may apply spring RetryTemplate
                    } else {
                        throw ex;
                    }
                }
            }
    }

    private Destination getReplyToDestination(Object destination) {
        if (destination instanceof Destination) {
            return (Destination) destination;
        } else {
            return null;
        }
    }

    private String getReplyToDestinationName(Object destination) {
        if (destination instanceof String) {
            return(String) destination;
        } else {
            return null;
        }
    }

    public JmsMQMessage sendAndReceive(final String destination, final JmsMQMessage message) throws JmsException {

        Assert.notNull(message, "argument 'message' is null");
        Assert.notNull(message.getPayload(), "payload of argument 'message' is null");
        final Object replyTo = message.getHeader(JmsMQMessage.JMS_REPLY_TO);

        return doSendAndReceive(message, jmsTemplate -> {

            final AtomicReference<Message> m = new AtomicReference<>();

            jmsTemplate.send(destination, session -> {

                Message jmsMessage = null;
                MessageConverter messageConverter = getMessageConverter();
                if (messageConverter != null) {
                    jmsMessage = messageConverter.toMessage(message.getPayload(), session);
                } else {
                    jmsMessage = session.createTextMessage(message.getPayload());
                }
                JmsMQMessage.populateJmsHeaders(message, jmsMessage);

                if (getReplyToDestination(replyTo) != null) {
                    jmsMessage.setJMSReplyTo(getReplyToDestination(replyTo));
                } else if (getReplyToDestinationName(replyTo) != null) {
                    String name = getReplyToDestinationName(replyTo);
                    Destination replyToDestination = jmsTemplate.getDestinationResolver().resolveDestinationName(session, name, false);
                    jmsMessage.setJMSReplyTo(replyToDestination);
                }

                m.set(jmsMessage);
                return jmsMessage;
            });

            if (messageSentCallback != null) {
                messageSentCallback.messageSent(JmsMQMessage.fromJmsMessage(m.get()));
            }

            String correlationId = null;
            try {
                correlationId = m.get().getJMSCorrelationID();
                if (correlationId == null) {
                    correlationId = m.get().getJMSMessageID();
                }
            } catch (JMSException ex) {
                log.debug("failed to read JMSCorrelationID or JMSMessageID from the JMS message");
            }

            if (correlationId == null) {
                return null;
            }
            String selector = String.format("%s='%s'", JmsMQMessage.JMS_CORRELATION_ID, correlationId);

            if (jmsTemplate.getReceiveTimeout() == 0) {
                log.warn("receive timeout value is '0', this will cause the JmsTemplate to wait indefinitely");
            }

            Message jmsMessage = null;
            if (getReplyToDestination(replyTo) != null) {
                jmsMessage = jmsTemplate.receiveSelected(getReplyToDestination(replyTo), selector);
            } else if (getReplyToDestinationName(replyTo) != null) {
                jmsMessage = jmsTemplate.receiveSelected(getReplyToDestinationName(replyTo), selector);
            } else {
                jmsTemplate.receiveSelected(selector);
            }

            if (jmsMessage == null) {
                String msg = String.format("timeout after waiting %d (ms) for %s", getJmsTemplate().getReceiveTimeout(), selector);
                throw new JmsTimeoutException(msg);
            }

            JmsMQMessage messageReceived = JmsMQMessage.fromJmsMessage(jmsMessage);
            if (messageReceivedCallback != null) {
                messageReceivedCallback.messageReceived(messageReceived);
            }

            return messageReceived;
        });
    }

    public void send(final Destination destination, final JmsMQMessage message) throws JmsException {

        Assert.notNull(message, "argument 'message' is null");
        Assert.notNull(message.getPayload(), "payload of argument 'message' is null");

        doSend(message, jmsTemplate -> {

            final AtomicReference<Message> m = new AtomicReference<>();

            jmsTemplate.send(destination, session -> {

                Message jmsMessage = null;
                MessageConverter messageConverter = getMessageConverter();
                if (messageConverter != null) {
                    jmsMessage = messageConverter.toMessage(message.getPayload(), session);
                } else {
                    jmsMessage = session.createTextMessage(message.getPayload());
                }
                JmsMQMessage.populateJmsHeaders(message, jmsMessage);

                m.set(jmsMessage);
                return jmsMessage;
            });

            if (messageSentCallback != null) {
                messageSentCallback.messageSent(JmsMQMessage.fromJmsMessage(m.get()));
            }

        });

    }

    public void send(final String destination, final JmsMQMessage message) throws JmsException {

        Assert.notNull(message, "argument 'message' is null");
        Assert.notNull(message.getPayload(), "payload of argument 'message' is null");

        doSend(message, jmsTemplate -> {

            final AtomicReference<Message> m = new AtomicReference<>();

            jmsTemplate.send(destination, session -> {

                Message jmsMessage = null;
                MessageConverter messageConverter = getMessageConverter();
                if (messageConverter != null) {
                    jmsMessage = messageConverter.toMessage(message.getPayload(), session);
                } else {
                    jmsMessage = session.createTextMessage(message.getPayload());
                }
                JmsMQMessage.populateJmsHeaders(message, jmsMessage);

                m.set(jmsMessage);
                return jmsMessage;
            });

            if (messageSentCallback != null) {
                messageSentCallback.messageSent(JmsMQMessage.fromJmsMessage(m.get()));
            }

        });

    }
    
}
