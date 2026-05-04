package io.agh.iot.stream.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {

    @Bean
    RestClient alertApiRestClient(RestClient.Builder builder, StreamProperties streamProperties) {
        return builder.baseUrl(streamProperties.getAlertApiBaseUrl()).build();
    }
}
