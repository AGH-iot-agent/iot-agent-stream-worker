package io.agh.iot.stream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "stream")
public class StreamProperties {

    private String mqttBrokerUri = "tcp://iot-agent-mqtt-broker:1883";
    private String alertApiBaseUrl = "http://iot-agent-alert-api:8080";

    public String getMqttBrokerUri() {
        return mqttBrokerUri;
    }

    public void setMqttBrokerUri(String mqttBrokerUri) {
        this.mqttBrokerUri = mqttBrokerUri;
    }

    public String getAlertApiBaseUrl() {
        return alertApiBaseUrl;
    }

    public void setAlertApiBaseUrl(String alertApiBaseUrl) {
        this.alertApiBaseUrl = alertApiBaseUrl;
    }
}
