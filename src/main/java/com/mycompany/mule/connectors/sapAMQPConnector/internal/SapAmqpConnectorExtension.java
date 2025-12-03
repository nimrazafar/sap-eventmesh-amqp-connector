package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;
// Removed JavaVersion imports
import org.mule.sdk.api.annotation.JavaVersionSupport;
import static org.mule.sdk.api.meta.JavaVersion.*;

@Xml(prefix = "sap-amqp")
@Extension(name = "sap-amqp-connector")
@Configurations(SapAmqpConnectorConfiguration.class)
@JavaVersionSupport({JAVA_17})

// Removed @JavaVersionSupport annotation
public class SapAmqpConnectorExtension {

}