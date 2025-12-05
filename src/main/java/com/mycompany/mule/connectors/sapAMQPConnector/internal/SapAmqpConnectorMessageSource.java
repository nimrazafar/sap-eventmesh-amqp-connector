package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.annotation.param.NullSafe;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;
import org.mule.runtime.extension.api.runtime.operation.Result;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.*;
import org.apache.qpid.jms.JmsConnectionFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Message Source for consuming messages from SAP Event Mesh queues
 */
@Alias("listener")
@DisplayName("Message Listener")
@Summary("Listens for messages from SAP Event Mesh queue")
@MediaType(value = ANY, strict = false)
public class SapAmqpConnectorMessageSource extends Source<String, Map<String, Object>> {

    private final Logger LOGGER = LoggerFactory.getLogger(SapAmqpConnectorMessageSource.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Config
    private SapAmqpConnectorConfiguration config;

    @Connection
    private ConnectionProvider<SapAmqpConnectorConnection> connectionProvider;
    
    @Parameter
    @Optional()
    @DisplayName("Queue Name")
    @Summary("Name of the queue to listen to") 
    String queueName;

    @Parameter
    @Optional(defaultValue = "1")
    @DisplayName("Number of Consumers")
    @Summary("Number of concurrent message consumers (default: 1)")
    private int numberOfConsumers;

    @Parameter
    @Optional(defaultValue = "AUTO")
    @DisplayName("Acknowledgment Mode")
    @Summary("Message acknowledgment mode: AUTO, CLIENT, or DUPS_OK")
    private String ackMode;

    @Parameter
    @Optional
    @DisplayName("Message Selector")
    @Summary("JMS message selector for filtering messages (optional)")
    private String messageSelector;

    @Parameter
    @Optional 
    @DisplayName("Headers") 
    @Summary("Custom headers including Authorization token") 
    @NullSafe 
    private List<MessageHeader> headers;

    private jakarta.jms.Connection jmsConnection;
    private List<ConsumerWorker> consumers = new ArrayList<>();

    @Override
    public void onStart(SourceCallback<String, Map<String, Object>> sourceCallback) {
        LOGGER.info("=== Starting SAP Event Mesh Message Listener ===");
        running.set(true);

        try {
            // Get connection from provider
            SapAmqpConnectorConnection muleConnection = connectionProvider.connect();
            
            // --- Get Access Token from Headers ---
            String accessToken = null;
            
            if (headers != null) {
                for (MessageHeader header : headers) {
                    if (header.getKey() != null) {
                        String key = header.getKey().trim();
                        if ("Authorization".equalsIgnoreCase(key) || "Authorisation".equalsIgnoreCase(key)) {
                            accessToken = header.getValue();
                            LOGGER.debug("Using {} token from headers", key);
                            break;
                        }
                    }
                }
            }

            if (accessToken == null || accessToken.trim().isEmpty()) {
                LOGGER.error("Authorization token must be provided in headers for message listener");
                return;
            }
            
            muleConnection.setAccessToken(accessToken, 3600);
            LOGGER.info("OAuth2 token obtained for message consumption");

            // Build AMQP WebSocket Connection URL
            String connectionUrl = buildConnectionUrl(config.getUri(), accessToken);
            LOGGER.info("Connection URL built for consumer");

            // Create JMS Connection Factory
            JmsConnectionFactory factory = new JmsConnectionFactory();
            factory.setRemoteURI(connectionUrl);
            factory.setUsername(config.getClientId());
            factory.setPassword(accessToken);

            // Create and start JMS connection
            jmsConnection = factory.createConnection();
            jmsConnection.start();
            muleConnection.setJmsConnection(jmsConnection);
            LOGGER.info("JMS Connection established for message consumption");

            // Determine acknowledgment mode
            int sessionAckMode = getAcknowledgmentMode();

            // Start consumer threads
            for (int i = 0; i < numberOfConsumers; i++) {
                ConsumerWorker worker = new ConsumerWorker(
                    jmsConnection, 
                    queueName, 
                    sourceCallback, 
                    sessionAckMode,
                    messageSelector,
                    i + 1
                );
                consumers.add(worker);
                new Thread(worker, "SAP-AMQP-Consumer-" + (i + 1)).start();
                LOGGER.info("Started consumer thread #{}", i + 1);
            }

            LOGGER.info("Message Listener started successfully with {} consumer(s)", numberOfConsumers);

        } catch (Exception e) {
            LOGGER.error("Failed to start message listener", e);
            onStop();
        }
    }

    @Override
    public void onStop() {
        LOGGER.info("=== Stopping SAP Event Mesh Message Listener ===");
        running.set(false);

        // Stop all consumers
        for (ConsumerWorker worker : consumers) {
            worker.stop();
        }
        consumers.clear();

        // Close JMS connection
        if (jmsConnection != null) {
            try {
                jmsConnection.close();
                LOGGER.info("JMS Connection closed");
            } catch (JMSException e) {
                LOGGER.error("Error closing JMS connection", e);
            }
            jmsConnection = null;
        }

        LOGGER.info("Message Listener stopped successfully");
    }

    /**
     * Worker class that consumes messages in a separate thread
     */
    private class ConsumerWorker implements Runnable {
        private final jakarta.jms.Connection jmsConn;
        private final String queueName;
        private final SourceCallback<String, Map<String, Object>> callback;
        private final int sessionAckMode;
        private final String selector;
        private final int workerId;
        private volatile boolean active = true;

        public ConsumerWorker(jakarta.jms.Connection jmsConn, String queueName, 
                            SourceCallback<String, Map<String, Object>> callback,
                            int sessionAckMode, String selector, int workerId) {
            this.jmsConn = jmsConn;
            this.queueName = queueName;
            this.callback = callback;
            this.sessionAckMode = sessionAckMode;
            this.selector = selector;
            this.workerId = workerId;
        }

        @Override
        public void run() {
            Session session = null;
            MessageConsumer consumer = null;

            try {
                LOGGER.info("Consumer #{} initializing...", workerId);
                
                // Create session
                session = jmsConn.createSession(
                    sessionAckMode == Session.CLIENT_ACKNOWLEDGE,
                    sessionAckMode
                );
                LOGGER.debug("Consumer #{} - Session created", workerId);

                // Create queue destination
                Destination queue = session.createQueue(queueName);
                LOGGER.debug("Consumer #{} - Queue destination: {}", workerId, queueName);

                // Create consumer with or without selector
                if (selector != null && !selector.trim().isEmpty()) {
                    consumer = session.createConsumer(queue, selector);
                    LOGGER.info("Consumer #{} - Created with message selector: {}", workerId, selector);
                } else {
                    consumer = session.createConsumer(queue);
                    LOGGER.info("Consumer #{} - Created without message selector", workerId);
                }

                LOGGER.info("Consumer #{} ready and listening for messages...", workerId);

                // Message consumption loop
                while (running.get() && active) {
                    try {
                        // Receive message with 1 second timeout
                        Message message = consumer.receive(1000);
                        
                        if (message != null) {
                            processMessage(message, session, sessionAckMode);
                        }
                    } catch (JMSException e) {
                        if (running.get() && active) {
                            LOGGER.error("Consumer #{} - Error receiving message", workerId, e);
                            Thread.sleep(1000); // Wait before retry
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Fatal error", workerId, e);
            } finally {
                // Cleanup resources
                try {
                    if (consumer != null) consumer.close();
                    if (session != null) session.close();
                    LOGGER.info("Consumer #{} - Resources cleaned up", workerId);
                } catch (JMSException e) {
                    LOGGER.error("Consumer #{} - Error closing resources", workerId, e);
                }
            }
        }

        /**
         * Process received JMS message and invoke callback
         */
        private void processMessage(Message message, Session session, int ackMode) {
            try {
                LOGGER.debug("Consumer #{} - Message received", workerId);

                // Extract message payload
                String payload = extractPayload(message);
                
                // Extract message attributes
                Map<String, Object> attributes = extractAttributes(message);
                
                // Log message details
                LOGGER.info("Consumer #{} - Processing message [ID: {}, Type: {}]", 
                    workerId, 
                    attributes.get("messageId"), 
                    attributes.get("messageType"));
                
                // Create result
                Result<String, Map<String, Object>> result = Result.<String, Map<String, Object>>builder()
                    .output(payload)
                    .attributes(attributes)
                    .build();

                // Invoke callback to pass message to Mule flow
                callback.handle(result);

                // Manual acknowledgment if CLIENT mode
                if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                    LOGGER.debug("Consumer #{} - Message acknowledged", workerId);
                }

                LOGGER.info("Consumer #{} - Message processed successfully", workerId);

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Error processing message", workerId, e);
                
                // On error, try to recover session
                try {
                    if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                        session.recover();
                        LOGGER.info("Consumer #{} - Session recovered", workerId);
                    }
                } catch (JMSException ex) {
                    LOGGER.error("Consumer #{} - Failed to recover session", workerId, ex);
                }
            }
        }

        /**
         * Extract message payload as String
         */
        private String extractPayload(Message message) throws JMSException {
            if (message instanceof TextMessage) {
                return ((TextMessage) message).getText();
            } else if (message instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) message;
                byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                bytesMessage.readBytes(bytes);
                return new String(bytes);
            } else if (message instanceof ObjectMessage) {
                Object obj = ((ObjectMessage) message).getObject();
                return obj != null ? obj.toString() : "";
            } else {
                return message.toString();
            }
        }

        /**
         * Extract message attributes (headers and properties)
         */
        private Map<String, Object> extractAttributes(Message message) throws JMSException {
            Map<String, Object> attributes = new HashMap<>();

            // Standard JMS headers
            attributes.put("messageId", message.getJMSMessageID());
            attributes.put("timestamp", message.getJMSTimestamp());
            attributes.put("correlationId", message.getJMSCorrelationID());
            attributes.put("replyTo", message.getJMSReplyTo() != null ? message.getJMSReplyTo().toString() : null);
            attributes.put("destination", message.getJMSDestination() != null ? message.getJMSDestination().toString() : null);
            attributes.put("deliveryMode", message.getJMSDeliveryMode());
            attributes.put("redelivered", message.getJMSRedelivered());
            attributes.put("type", message.getJMSType());
            attributes.put("expiration", message.getJMSExpiration());
            attributes.put("priority", message.getJMSPriority());
            
            // Determine message type
            String messageType = "Unknown";
            if (message instanceof TextMessage) {
                messageType = "TextMessage";
            } else if (message instanceof BytesMessage) {
                messageType = "BytesMessage";
            } else if (message instanceof ObjectMessage) {
                messageType = "ObjectMessage";
            } else if (message instanceof MapMessage) {
                messageType = "MapMessage";
            } else if (message instanceof StreamMessage) {
                messageType = "StreamMessage";
            }
            attributes.put("messageType", messageType);

            // Custom properties
            java.util.Enumeration<?> propertyNames = message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                String propertyName = (String) propertyNames.nextElement();
                Object propertyValue = message.getObjectProperty(propertyName);
                attributes.put("property_" + propertyName, propertyValue);
            }

            return attributes;
        }

        public void stop() {
            active = false;
            LOGGER.info("Consumer #{} - Stop signal received", workerId);
        }
    }

    /**
     * Get JMS acknowledgment mode from string parameter
     */
    private int getAcknowledgmentMode() {
        if (ackMode == null) {
            return Session.AUTO_ACKNOWLEDGE;
        }

        switch (ackMode.toUpperCase()) {
            case "CLIENT":
                LOGGER.info("Using CLIENT_ACKNOWLEDGE mode");
                return Session.CLIENT_ACKNOWLEDGE;
            case "DUPS_OK":
                LOGGER.info("Using DUPS_OK_ACKNOWLEDGE mode");
                return Session.DUPS_OK_ACKNOWLEDGE;
            case "AUTO":
            default:
                LOGGER.info("Using AUTO_ACKNOWLEDGE mode");
                return Session.AUTO_ACKNOWLEDGE;
        }
    }

    /**
     * Build AMQP WebSocket connection URL with OAuth token
     */
    private String buildConnectionUrl(String wsUri, String accessToken) throws Exception {
        try {
            URI uri = new URI(wsUri);
            String protocol = uri.getScheme();
            String host = uri.getHost();
            String path = uri.getPath();
            int port = uri.getPort();
            
            String amqpProtocol;
            int targetPort;
            
            if ("wss".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpwss";
                targetPort = (port > 0) ? port : 443;
            } else if ("ws".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpws";
                targetPort = (port > 0) ? port : 80;
            } else if ("amqpwss".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpwss";
                targetPort = (port > 0) ? port : 443;
            } else if ("amqpws".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpws";
                targetPort = (port > 0) ? port : 80;
            } else {
                throw new ConnectionException("Unsupported protocol: " + protocol);
            }
            
            // URL encode the Bearer token
            String encodedToken = URLEncoder.encode("Bearer " + accessToken, "UTF-8");
            
            // Build connection URL with Authorization header
            return String.format(
                "%s://%s:%d%s?transport.tcpNoDelay=true&transport.ws.httpHeader.Authorization=%s",
                amqpProtocol, host, targetPort, path, encodedToken
            );
            
        } catch (Exception e) {
            LOGGER.error("Error parsing URI: {}", wsUri, e);
            throw new ConnectionException("Invalid URI format: " + wsUri, e);
        }
    }
}