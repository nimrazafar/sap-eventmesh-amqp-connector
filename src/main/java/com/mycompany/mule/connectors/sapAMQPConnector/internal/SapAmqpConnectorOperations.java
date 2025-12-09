package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;
import java.nio.charset.StandardCharsets;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.NullSafe;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;

import jakarta.jms.*;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.mule.runtime.api.connection.ConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;
import java.net.URLEncoder;
import java.io.InputStream;

public class SapAmqpConnectorOperations {

    private final Logger LOGGER = LoggerFactory.getLogger(SapAmqpConnectorOperations.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @DisplayName("Publish")
    @MediaType(value = ANY, strict = false)
    public void publishMessage(
            @Config SapAmqpConnectorConfiguration config,
            @Connection SapAmqpConnectorConnection connection,
            @DisplayName("Queue Name") @Summary("Name of the queue to publish to") String queueName,
            @Content @DisplayName("Message Payload") Object payload,
            @Optional @DisplayName("Headers") @Summary("Custom headers to add to the message") 
            @NullSafe List<MessageHeader> headers) throws ConnectionException {

        jakarta.jms.Connection jmsConnection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            LOGGER.info("=== Starting SAP Event Mesh Publish ===");

            // Extract and validate access token
            String accessToken = extractAccessToken(headers);
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new ConnectionException("Authorization token must be provided in headers.");
            }

            // Convert payload to string
            String messagePayload = convertPayloadToString(payload);
            LOGGER.info("Message payload converted to String (length: {})", messagePayload.length());

            // Create and start JMS connection
            jmsConnection = createJmsConnection(config, accessToken, connection);

            // Create session and producer
            session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(queueName);
            producer = session.createProducer(queue);
            LOGGER.debug("Message Producer created for queue: {}", queueName);

            // Create and send message
            TextMessage msg = session.createTextMessage(messagePayload);
            addCustomHeaders(msg, headers);
            producer.send(msg);
            
            LOGGER.info("Successfully published message to queue: {}", queueName);

        } catch (JMSException e) {
            handleJmsException(e, connection);
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            if (e instanceof ConnectionException) {
                throw (ConnectionException) e;
            }
            throw new RuntimeException("Unexpected error: " + e.getMessage(), e);
        } finally {
            closeResources(producer, session, connection);
        }
    }

    @DisplayName("Consume")
    @MediaType(value = ANY, strict = false)
    @Summary("Synchronously consume a single message from SAP Event Mesh queue")
    public String consumeMessage(
            @Config SapAmqpConnectorConfiguration config,
            @Connection SapAmqpConnectorConnection connection,
            @DisplayName("Queue Name") @Summary("Name of the queue to consume from") String queueName,
            @Optional(defaultValue = "5000") @DisplayName("Timeout (ms)") 
            @Summary("Time to wait for a message in milliseconds (default: 5000ms)") long timeout,
            @Optional @DisplayName("Headers") @Summary("Custom headers including Authorization token") 
            @NullSafe List<MessageHeader> headers) {

        jakarta.jms.Connection jmsConnection = null;
        Session session = null;
        MessageConsumer consumer = null;

        try {
            LOGGER.info("=== Starting SAP Event Mesh Message Consumption ===");

            // Extract and validate access token
            String accessToken = extractAccessToken(headers);
            if (accessToken == null || accessToken.trim().isEmpty()) {
                return buildErrorResponse("AUTHENTICATION_ERROR", "Authorization token must be provided in headers");
            }

            // Create and start JMS connection
            jmsConnection = createJmsConnection(config, accessToken, connection);

            // Create session and consumer
            session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(queueName);
            consumer = session.createConsumer(queue);
            LOGGER.debug("Message Consumer created for queue: {}", queueName);

            // Receive message with timeout
            LOGGER.info("Waiting for message (timeout: {}ms)...", timeout);
            Message message = consumer.receive(timeout);

            if (message == null) {
                LOGGER.warn("No message received within timeout period");
                return buildNoMessageResponse(timeout);
            }

            // Extract message details and build response
            LOGGER.info("Message received from queue");
            String payloadStr = extractPayload(message);
            Map<String, Object> attributes = extractAttributes(message);
            
            LOGGER.info("Message ID: {}, Type: {}", attributes.get("messageId"), attributes.get("messageType"));
            
            return buildSuccessResponse(payloadStr, attributes);

        } catch (JMSException e) {
            return handleJmsExceptionForConsume(e, connection);
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            return buildErrorResponse("ERROR", "Unexpected error: " + e.getMessage());
        } finally {
            closeResources(consumer, session, connection);
        }
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    private String extractAccessToken(List<MessageHeader> headers) {
        if (headers == null) return null;
        
        for (MessageHeader header : headers) {
            if (header.getKey() != null && "Authorization".equalsIgnoreCase(header.getKey().trim())) {
                return header.getValue();
            }
        }
        return null;
    }

    private jakarta.jms.Connection createJmsConnection(
            SapAmqpConnectorConfiguration config, 
            String accessToken,
            SapAmqpConnectorConnection connection) throws Exception {
        
        LOGGER.debug("Creating JMS connection...");
        
        String connectionUrl = buildConnectionUrl(config.getUri(), accessToken);
        
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.setRemoteURI(connectionUrl);
        factory.setUsername(config.getClientId());
        factory.setPassword(accessToken);

        jakarta.jms.Connection jmsConnection = factory.createConnection();
        connection.setJmsConnection(jmsConnection);
        jmsConnection.start();
        
        LOGGER.info("JMS Connection started successfully");
        return jmsConnection;
    }

    private String buildConnectionUrl(String wsUri, String accessToken) throws Exception {
        try {
            URI uri = new URI(wsUri);
            String protocol = uri.getScheme();
            String host = uri.getHost();
            String path = uri.getPath();
            int port = uri.getPort();
            
            // Determine protocol and port
            String amqpProtocol;
            int targetPort;
            
            if ("wss".equalsIgnoreCase(protocol) || "amqpwss".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpwss";
                targetPort = (port > 0) ? port : 443;
            } else if ("ws".equalsIgnoreCase(protocol) || "amqpws".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpws";
                targetPort = (port > 0) ? port : 80;
            } else {
                throw new ConnectionException("Unsupported protocol: " + protocol);
            }
            
            String encodedToken = URLEncoder.encode(accessToken, StandardCharsets.UTF_8);
            
            return String.format(
                "%s://%s:%d%s?transport.tcpNoDelay=true&transport.ws.httpHeader.Authorization=%s",
                amqpProtocol, host, targetPort, path, encodedToken
            );
            
        } catch (Exception e) {
            LOGGER.error("Error parsing URI: {}", wsUri, e);
            throw new ConnectionException("Invalid URI format: " + wsUri, e);
        }
    }

    private void addCustomHeaders(TextMessage msg, List<MessageHeader> headers) throws JMSException {
        if (headers == null || headers.isEmpty()) return;
        
        int headerCount = 0;
        for (MessageHeader header : headers) {
            // Skip authorization header
            if ("Authorization".equalsIgnoreCase(header.getKey())) {
                continue;
            }
            
            String headerName = header.getKey();
            String headerValue = header.getValue();
            
            if (headerName != null && !headerName.trim().isEmpty()) {
                try {
                    msg.setStringProperty(headerName.trim(), headerValue);
                    headerCount++;
                    LOGGER.debug("Added header: {} = {}", headerName, headerValue);
                } catch (JMSException e) {
                    LOGGER.warn("Failed to set header '{}': {}", headerName, e.getMessage());
                }
            }
        }
        
        if (headerCount > 0) {
            LOGGER.debug("Added {} custom headers to message", headerCount);
        }
    }

    private String extractPayload(Message message) throws Exception {
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        } else if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
            bytesMessage.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } else if (message instanceof ObjectMessage) {
            Object obj = ((ObjectMessage) message).getObject();
            return obj != null ? obj.toString() : "";
        } else {
            return message.toString();
        }
    }

    private Map<String, Object> extractAttributes(Message message) throws Exception {
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

    private String convertPayloadToString(Object payload) throws Exception {
        if (payload == null) {
            return "";
        }
        
        if (payload instanceof String) {
            return (String) payload;
        }
        
        if (payload instanceof byte[]) {
            return new String((byte[]) payload, StandardCharsets.UTF_8);
        }
        
        if (payload instanceof InputStream) {
            try (InputStream is = (InputStream) payload) {
                return new String(readAllBytes(is), StandardCharsets.UTF_8);
            }
        }
        
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            LOGGER.warn("Failed to serialize payload to JSON, using toString(): {}", e.getMessage());
            return payload.toString();
        }
    }
    
    private byte[] readAllBytes(InputStream is) throws Exception {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    private String buildSuccessResponse(String payload, Map<String, Object> attributes) throws Exception {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "SUCCESS");
        result.put("payload", payload);
        result.put("messageId", attributes.get("messageId"));
        result.put("timestamp", attributes.get("timestamp"));
        result.put("correlationId", attributes.get("correlationId"));
        result.put("messageType", attributes.get("messageType"));
        result.put("redelivered", attributes.get("redelivered"));
        result.put("priority", attributes.get("priority"));
        result.put("allAttributes", attributes);
        
        return objectMapper.writeValueAsString(result);
    }

    private String buildNoMessageResponse(long timeout) {
        try {
            Map<String, Object> result = new HashMap<>();
            result.put("status", "NO_MESSAGE");
            result.put("message", "No message available in queue within timeout period");
            result.put("timeout", timeout);
            return objectMapper.writeValueAsString(result);
        } catch (Exception e) {
            return "{\"status\":\"NO_MESSAGE\",\"timeout\":" + timeout + "}";
        }
    }

    private String buildErrorResponse(String status, String errorMessage) {
        try {
            Map<String, Object> error = new HashMap<>();
            error.put("status", status);
            error.put("error", errorMessage);
            return objectMapper.writeValueAsString(error);
        } catch (Exception e) {
            return "{\"status\":\"ERROR\",\"error\":\"" + errorMessage.replace("\"", "\\\"") + "\"}";
        }
    }

    private void handleJmsException(JMSException e, SapAmqpConnectorConnection connection) 
            throws ConnectionException {
        String errorMessage = (e.getMessage() != null) ? e.getMessage().toLowerCase() : "";
        LOGGER.error("JMS Error: {}", e.getMessage());
        
        if (isAuthenticationError(errorMessage)) {
            LOGGER.warn("Authentication failed - invalidating token");
            connection.clearToken();
            throw new ConnectionException("Authentication failed: " + e.getMessage(), e);
        } else {
            LOGGER.error("JMSException occurred", e);
            throw new RuntimeException("Error during JMS operation: " + e.getMessage(), e);
        }
    }

    private String handleJmsExceptionForConsume(JMSException e, SapAmqpConnectorConnection connection) {
        String errorMessage = (e.getMessage() != null) ? e.getMessage().toLowerCase() : "";
        LOGGER.error("JMS Error: {}", e.getMessage());
        
        if (isAuthenticationError(errorMessage)) {
            LOGGER.warn("Authentication failed - invalidating token");
            connection.clearToken();
            return buildErrorResponse("AUTHENTICATION_ERROR", "Authentication failed: " + e.getMessage());
        } else {
            LOGGER.error("JMSException occurred", e);
            return buildErrorResponse("JMS_ERROR", "Error during JMS operation: " + e.getMessage());
        }
    }

    private boolean isAuthenticationError(String errorMessage) {
        return errorMessage.contains("unauthorized") || 
               errorMessage.contains("401") ||
               errorMessage.contains("authentication failed") || 
               errorMessage.contains("forbidden") || 
               errorMessage.contains("security exception");
    }

    private void closeResources(AutoCloseable resource1, AutoCloseable resource2, 
            SapAmqpConnectorConnection connection) {
        LOGGER.debug("Closing JMS resources...");
        
        if (resource1 != null) {
            try { 
                resource1.close(); 
            } catch (Exception e) { 
                LOGGER.error("Error closing resource", e); 
            }
        }
        
        if (resource2 != null) {
            try { 
                resource2.close(); 
            } catch (Exception e) { 
                LOGGER.error("Error closing resource", e); 
            }
        }
        
        try { 
            connection.disconnect(); 
            LOGGER.debug("JMS Connection closed"); 
        } catch (Exception e) { 
            LOGGER.error("Error closing connection", e); 
        }
    }
}