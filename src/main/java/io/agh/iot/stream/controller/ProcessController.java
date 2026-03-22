package io.agh.iot.stream.controller;

import lombok.Data;
import org.springframework.web.bind.annotation.*;

@RestController
public class ProcessController {

    @PostMapping("/process")
    public ProcessedEvent processEvent(@RequestBody RawEvent event) {
        double temperature = event.getTemperature();
        return new ProcessedEvent(
            event.getDeviceId(),
            Math.round(temperature * 100.0) / 100.0,
            Math.round(event.getHumidity() * 100.0) / 100.0,
            temperature > 30
        );
    }

    @Data
    static class RawEvent {
        private String deviceId;
        private double temperature;
        private double humidity;
    }

    record ProcessedEvent(String deviceId, double temperatureC, double humidityPct, boolean isHot) {}
}
