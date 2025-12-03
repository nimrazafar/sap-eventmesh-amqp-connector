package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SapAmqpConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(SapAmqpConnectorConnection.class);

    // Store token and expiry time manually
    private String currentToken;
    private Instant tokenExpiryTime; // For proactive refresh
    private Connection jmsConnection;

    // Default constructor
    public SapAmqpConnectorConnection() {}

    // --- Manual Token Management ---
    
    /**
     * Get the current access token if it's still valid
     * @return current token or null if expired/not set
     */
    public String getAccessToken() {
        // Check expiry time if set
        if (tokenExpiryTime != null && Instant.now().isAfter(tokenExpiryTime)) {
            LOGGER.debug("Token has expired, returning null to trigger refresh");
            return null; // Signal that token needs refresh
        }
        return this.currentToken;
    }

    /**
     * Set a new access token with its expiry time
     * @param token The access token
     * @param expiresInSeconds How long the token is valid (in seconds)
     */
    public void setAccessToken(String token, long expiresInSeconds) {
        this.currentToken = token;
        if (expiresInSeconds > 0) {
            // Refresh token 60 seconds before actual expiry to avoid edge cases
            this.tokenExpiryTime = Instant.now().plusSeconds(expiresInSeconds - 60);
            LOGGER.debug("Token set with expiry at: {}", this.tokenExpiryTime);
        } else {
            this.tokenExpiryTime = null; // No expiry info
            LOGGER.debug("Token set without expiry time");
        }
    }
    
    /**
     * Check if the token is still valid
     * @return true if token exists and hasn't expired
     */
    public boolean isTokenValid() {
        return this.currentToken != null && 
               (this.tokenExpiryTime == null || Instant.now().isBefore(this.tokenExpiryTime));
    }
    
    /**
     * Clear the current token
     */
    public void clearToken() {
        this.currentToken = null;
        this.tokenExpiryTime = null;
        LOGGER.debug("Token cleared");
    }
    
    // --- End Manual Token Management ---


    // --- JMS Connection Management ---
    
    /**
     * Set the JMS connection
     * @param jmsConnection The Jakarta JMS Connection
     */
    public void setJmsConnection(Connection jmsConnection) {
        this.jmsConnection = jmsConnection;
    }

    /**
     * Get the current JMS connection
     * @return The Jakarta JMS Connection or null if not set
     */
    public Connection getJmsConnection() {
        return this.jmsConnection;
    }

    /**
     * Check if a JMS connection is currently active
     * @return true if connection exists
     */
    public boolean isConnected() {
        return this.jmsConnection != null;
    }

    /**
     * Disconnect and close the JMS connection
     * @throws Exception if there's an error closing the connection
     */
    public void disconnect() throws Exception {
        if (this.jmsConnection != null) {
            try {
                LOGGER.debug("Closing JMS connection");
                this.jmsConnection.close();
                LOGGER.debug("JMS connection closed successfully");
            } catch (JMSException e) {
                LOGGER.error("Error closing JMS connection", e);
                throw e;
            } finally {
                this.jmsConnection = null;
                // Optionally clear token on disconnect if needed
                // this.clearToken();
            }
        }
    }
    
    /**
     * Invalidate the connection (for error scenarios)
     */
    public void invalidate() {
        try {
            disconnect();
        } catch (Exception e) {
            LOGGER.error("Error during connection invalidation", e);
        }
        clearToken();
    }
    
    // --- End JMS Connection Management ---
}