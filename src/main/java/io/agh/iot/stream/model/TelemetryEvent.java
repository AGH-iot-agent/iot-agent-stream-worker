package io.agh.iot.stream.model;

public record TelemetryEvent(
    String deviceId,
    long timestamp,
    Double temperatureC,
    Double humidityPct,
    Double co2Ppm,
    Double pm25,
    Double energyUsageW,
    String krakowZone,
    boolean leak
) {
}
