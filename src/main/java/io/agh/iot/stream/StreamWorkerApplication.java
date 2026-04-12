package io.agh.iot.stream;

import io.agh.iot.stream.config.StreamProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;


@SpringBootApplication
@EnableConfigurationProperties(StreamProperties.class)
public class StreamWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamWorkerApplication.class, args);
    }
}
