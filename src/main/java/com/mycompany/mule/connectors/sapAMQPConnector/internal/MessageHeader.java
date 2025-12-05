package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.sdk.api.annotation.Expression;
import org.mule.sdk.api.annotation.param.display.Text;
import org.mule.sdk.api.meta.ExpressionSupport;
import org.mule.runtime.extension.api.annotation.param.Parameter;

public class MessageHeader {
    
    private static final ExpressionSupport SUPPORTED = null;

	@Parameter
    @DisplayName("Key")
    @Summary("Header key name")
    @Text
    private String key;
    
    @Parameter
    @DisplayName("Value")
    @Summary("Header value (supports expressions)")
    @Expression(ExpressionSupport.SUPPORTED)
    private String value;
    
    public String getKey() {
        return key;
    }
    
    public void setKey(String key) {
        this.key = key;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
}