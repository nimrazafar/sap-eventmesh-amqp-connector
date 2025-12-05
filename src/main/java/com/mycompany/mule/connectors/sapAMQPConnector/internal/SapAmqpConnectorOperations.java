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

import jakarta.jms.ConnectionFactory;
import jakarta.jms.Session;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.MessageConsumer;
import jakarta.jms.TextMessage;
import jakarta.jms.Message;
import jakarta.jms.BytesMessage;
import jakarta.jms.ObjectMessage;
import jakarta.jms.JMSException;
import org.apache.qpid.jms.JmsConnectionFactory;

import org.mule.runtime.api.connection.ConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Imports for Manual HTTP Token Request
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64;
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
            LOGGER.info("=== Starting SAP Event Mesh Connection ===");

            // --- Convert payload to String ---
            String messagePayload = convertPayloadToString(payload);
            LOGGER.info("Message payload converted to String (length: {})", messagePayload.length());

            // --- Get Access Token from Headers ---
            String accessToken = null;
            MessageHeader authHeader = null;
            
            if (headers != null) {
                for (MessageHeader header : headers) {
                    if (header.getKey() != null) {
                        String key = header.getKey().trim();
                        if ("Authorization".equalsIgnoreCase(key)) {
                            accessToken = header.getValue();
                            authHeader = header;
                            LOGGER.debug("Using {} token from headers", key);
                            break;
                        }
                    }
                }
            }
      
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new ConnectionException("Authorization token must be provided in headers.");
            }
            
            LOGGER.debug("Using OAuth2 access token for authentication");

            // --- Build AMQP WebSocket Connection URL with Explicit Port ---
            String wsUri = config.getUri();
            String connectionUrl;
            
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
                
                // URL encode the Bearer token for the Authorization header
                String encodedToken = URLEncoder.encode("Bearer " + accessToken, StandardCharsets.UTF_8);
                
                // Build connection URL with Authorization header
                connectionUrl = String.format(
                    "%s://%s:%d%s?transport.tcpNoDelay=true&transport.ws.httpHeader.Authorization=%s",
                    amqpProtocol, host, targetPort, path, encodedToken
                );
                
                LOGGER.info("Converted WebSocket URL: {}", 
                    connectionUrl.substring(0, Math.min(100, connectionUrl.length())) + "...");
                
            } catch (Exception e) {
                LOGGER.error("Error parsing URI: {}", wsUri, e);
                throw new ConnectionException("Invalid URI format: " + wsUri, e);
            }

            // --- Create JMS Connection Factory ---
            LOGGER.debug("Creating JMS Connection Factory...");
            JmsConnectionFactory factory = new JmsConnectionFactory();
            factory.setRemoteURI(connectionUrl);
            
            // Use client ID as username and token as password
            factory.setUsername(config.getClientId());
            factory.setPassword(accessToken);

            LOGGER.debug("Creating JMS Connection...");
            jmsConnection = factory.createConnection();
            connection.setJmsConnection(jmsConnection);
            jmsConnection.start();
            LOGGER.info("JMS Connection started successfully");

            // --- Create Session and Send Message ---
            session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            LOGGER.debug("JMS Session created");
            
            Destination queue = session.createQueue(queueName);
            LOGGER.debug("Destination queue: {}", queueName);
            
            producer = session.createProducer(queue);
            LOGGER.debug("Message Producer created");

            TextMessage msg = session.createTextMessage(messagePayload);
            
            // --- Add Custom Headers to JMS Message (excluding Authorization) ---
            if (headers != null && !headers.isEmpty()) {
                int headerCount = 0;
                for (MessageHeader header : headers) {
                    // Skip the authorization header
                    if (header == authHeader) {
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
            
            producer.send(msg);
            LOGGER.info("Successfully published message to queue: {}", queueName);

        } catch (JMSException e) {
            String errorMessage = (e.getMessage() != null) ? e.getMessage().toLowerCase() : "";
            LOGGER.error("JMS Error: {}", e.getMessage());
            
            if (errorMessage.contains("unauthorized") || 
                errorMessage.contains("401") ||
                errorMessage.contains("authentication failed") || 
                errorMessage.contains("forbidden") || 
                errorMessage.contains("security exception")) {
                LOGGER.warn("Authentication failed - invalidating token");
                connection.clearToken();
                throw new ConnectionException("Authentication failed: " + e.getMessage(), e);
            } else {
                LOGGER.error("JMSException occurred", e);
                throw new RuntimeException("Error during JMS operation: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            if (e instanceof ConnectionException) {
                throw (ConnectionException) e;
            }
            throw new RuntimeException("Unexpected error: " + e.getMessage(), e);
        } finally {
            LOGGER.debug("Closing JMS resources...");
            try { 
                if (producer != null) producer.close(); 
            } catch (JMSException e) { 
                LOGGER.error("Error closing producer", e); 
            }
            try { 
                if (session != null) session.close(); 
            } catch (JMSException e) { 
                LOGGER.error("Error closing session", e); 
            }
            try { 
                connection.disconnect(); 
                LOGGER.debug("JMS Connection closed"); 
            } catch (Exception e) { 
                LOGGER.error("Error closing connection", e); 
            }
        }
    }

    /**
     * Consume Message Operation - Synchronously receives one message from the queue
     * Returns JSON string with message details
     */
    @DisplayName("Consume")
    @MediaType(value = ANY, strict = false)
    @Summary("Synchronously consume a single message from SAP Event Mesh queue")
    public String consumeMessage(
            @Config SapAmqpConnectorConfiguration config,
            @Connection SapAmqpConnectorConnection connection,
            @DisplayName("Queue Name") 
            @Summary("Name of the queue to consume from") 
            String queueName,
            @Optional(defaultValue = "5000") @DisplayName("Timeout (ms)") 
            @Summary("Time to wait for a message in milliseconds (default: 5000ms)") 
            long timeout,
            @Optional @DisplayName("Headers") @Summary("Custom headers including Authorization token") 
            @NullSafe List<MessageHeader> headers) {

        jakarta.jms.Connection jmsConnection = null;
        Session session = null;
        MessageConsumer consumer = null;

        try {
            LOGGER.info("=== Starting SAP Event Mesh Message Consumption ===");

            // --- Get Access Token from Headers ---
            String accessToken = null;
            
            if (headers != null) {
                for (MessageHeader header : headers) {
                    if (header.getKey() != null) {
                        String key = header.getKey().trim();
                        if ("Authorization".equalsIgnoreCase(key)) {
                            accessToken = header.getValue();
                            LOGGER.debug("Using {} token from headers", key);
                            break;
                        }
                    }
                }
            }

            if (accessToken == null || accessToken.trim().isEmpty()) {
                return buildErrorResponse("AUTHENTICATION_ERROR", "Authorization token must be provided in headers");
            }
            LOGGER.debug("Using OAuth2 access token for authentication");

            // --- Build AMQP WebSocket Connection URL ---
            String connectionUrl = buildConnectionUrl(config.getUri(), accessToken);

            // --- Create JMS Connection Factory ---
            LOGGER.debug("Creating JMS Connection Factory...");
            JmsConnectionFactory factory = new JmsConnectionFactory();
            factory.setRemoteURI(connectionUrl);
            factory.setUsername(config.getClientId());
            factory.setPassword(accessToken);

            LOGGER.debug("Creating JMS Connection...");
            jmsConnection = factory.createConnection();
            connection.setJmsConnection(jmsConnection);
            jmsConnection.start();
            LOGGER.info("JMS Connection started successfully");

            // --- Create Session and Consumer ---
            session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            LOGGER.debug("JMS Session created");
            
            Destination queue = session.createQueue(queueName);
            LOGGER.debug("Destination queue: {}", queueName);
            
            consumer = session.createConsumer(queue);
            LOGGER.debug("Message Consumer created");

            // --- Receive Message with Timeout ---
            LOGGER.info("Waiting for message (timeout: {}ms)...", timeout);
            Message message = consumer.receive(timeout);

            if (message == null) {
                LOGGER.warn("No message received within timeout period");
                Map<String, Object> result = new HashMap<>();
                result.put("status", "NO_MESSAGE");
                result.put("message", "No message available in queue within timeout period");
                result.put("timeout", timeout);
                return objectMapper.writeValueAsString(result);
            }

            // --- Extract Message Details ---
            LOGGER.info("Message received from queue");
            
            String payload = extractPayload(message);
            Map<String, Object> attributes = extractAttributes(message);
            
            LOGGER.info("Message ID: {}", attributes.get("messageId"));
            LOGGER.info("Message Type: {}", attributes.get("messageType"));
            
            // --- Build Result ---
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

            LOGGER.info("Successfully consumed message from queue: {}", queueName);
            return objectMapper.writeValueAsString(result);

        } catch (JMSException e) {
            String errorMessage = (e.getMessage() != null) ? e.getMessage().toLowerCase() : "";
            LOGGER.error("JMS Error: {}", e.getMessage());
            
            if (errorMessage.contains("unauthorized") || 
                errorMessage.contains("401") ||
                errorMessage.contains("authentication failed") || 
                errorMessage.contains("forbidden") || 
                errorMessage.contains("security exception")) {
                LOGGER.warn("Authentication failed - invalidating token");
                connection.clearToken();
                return buildErrorResponse("AUTHENTICATION_ERROR", "Authentication failed: " + e.getMessage());
            } else {
                LOGGER.error("JMSException occurred", e);
                return buildErrorResponse("JMS_ERROR", "Error during JMS operation: " + e.getMessage());
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            return buildErrorResponse("ERROR", "Unexpected error: " + e.getMessage());
        } finally {
            LOGGER.debug("Closing JMS resources...");
            try { 
                if (consumer != null) consumer.close(); 
            } catch (JMSException e) { 
                LOGGER.error("Error closing consumer", e); 
            }
            try { 
                if (session != null) session.close(); 
            } catch (JMSException e) { 
                LOGGER.error("Error closing session", e); 
            }
            try { 
                connection.disconnect(); 
                LOGGER.debug("JMS Connection closed"); 
            } catch (Exception e) { 
                LOGGER.error("Error closing connection", e); 
            }
        }
    }

    /**
     * Build error response as JSON string
     */
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

    /**
     * Extract message payload as String
     */
    private String extractPayload(Message message) throws Exception {
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        } else if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
            bytesMessage.readBytes(bytes);
            return new String(bytes, "UTF-8");
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

    /**
     * Build AMQP WebSocket connection URL
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
            
            String encodedToken = URLEncoder.encode("Bearer " + accessToken, "UTF-8");
            
            return String.format(
                "%s://%s:%d%s?transport.tcpNoDelay=true&transport.ws.httpHeader.Authorization=%s",
                amqpProtocol, host, targetPort, path, encodedToken
            );
            
        } catch (Exception e) {
            LOGGER.error("Error parsing URI: {}", wsUri, e);
            throw new ConnectionException("Invalid URI format: " + wsUri, e);
        }
    }

    /**
     * Convert various payload types to String for JMS TextMessage
     */
    private String convertPayloadToString(Object payload) throws Exception {
        if (payload == null) {
            return "";
        }
        
        if (payload instanceof String) {
            return (String) payload;
        }
        
        if (payload instanceof byte[]) {
            return new String((byte[]) payload, "UTF-8");
        }
        
        if (payload instanceof InputStream) {
            try (InputStream is = (InputStream) payload) {
                return new String(readAllBytes(is), "UTF-8");
            }
        }
        
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            LOGGER.warn("Failed to serialize payload to JSON, using toString(): {}", e.getMessage());
            return payload.toString();
        }
    }
    
    /**
     * Read all bytes from InputStream
     */
    private byte[] readAllBytes(InputStream is) throws Exception {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }}