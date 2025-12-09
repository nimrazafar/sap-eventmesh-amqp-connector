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

    @Parameter
    @DisplayName("Client ID")
    private String clientId;

    public String getUri() { 
        return uri; 
    }

    public String getClientId() { 
        return clientId; 
    }
  
}