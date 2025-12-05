package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.Sources;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.mule.runtime.extension.api.annotation.param.display.Example;

@Operations(SapAmqpConnectorOperations.class)
@Sources(SapAmqpConnectorMessageSource.class)
@ConnectionProviders(SapAmqpConnectorConnectionProvider.class)
public class SapAmqpConnectorConfiguration {

    @Parameter
    @DisplayName("WebSocket URI")
    @Example("wss://your-instance.eu20.a.eventmesh.integration.cloud.sap/protocols/amqp10ws")
    private String uri;


//    @Parameter
//    @DisplayName("Token Endpoint URL")
//    @Example("https://your-subdomain.authentication.eu20.hana.ondemand.com/oauth/token")
//    private String tokenUrl;
//
    @Parameter
    @DisplayName("Client ID")
    private String clientId;

//    @Parameter
//    @DisplayName("Client Secret")
//    @Password
//    private String clientSecret;
//
    // Getters
    public String getUri() { 
        return uri; 
    }
//    
//    public String getTokenUrl() { 
//        return tokenUrl; 
//    }
    
    public String getClientId() { 
        return clientId; 
    }
    
//    public String getClientSecret() { 
//        return clientSecret; 
//    }
//    
}