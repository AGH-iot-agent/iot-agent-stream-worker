package io.agh.iot.stream.controller;

import io.agh.iot.stream.model.ProcessedSnapshot;
import io.agh.iot.stream.service.StreamAnalyticsService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class ProcessController {

    private final StreamAnalyticsService streamAnalyticsService;

    public ProcessController(StreamAnalyticsService streamAnalyticsService) {
        this.streamAnalyticsService = streamAnalyticsService;
    }

    @PostMapping("/process")
    public ProcessedEvent processEvent(@RequestBody RawEvent event) {
        double temperature = event.temperature();
        return new ProcessedEvent(
            event.deviceId(),
            Math.round(temperature * 100.0) / 100.0,
            Math.round(event.humidity() * 100.0) / 100.0,
            temperature > 30
        );
    }

    @GetMapping("/analytics/latest")
    public List<ProcessedSnapshot> latest() {
        return streamAnalyticsService.getLatestSnapshots();
    }

    @GetMapping("/analytics/series/{deviceId}")
    public List<ProcessedSnapshot> series(@PathVariable String deviceId) {
        return streamAnalyticsService.getSeriesForDevice(deviceId);
    }

    @GetMapping("/analytics/events-per-minute")
    public EventsRate eventsPerMinute() {
        return new EventsRate(streamAnalyticsService.getEventsPerMinuteEstimate());
    }

    public record RawEvent(String deviceId, double temperature, double humidity) {}

    public record ProcessedEvent(String deviceId, double temperatureC, double humidityPct, boolean isHot) {}
    public record EventsRate(int eventsPerMinute) {}
}
