package io.agh.iot.stream.model;

public record ProcessedSnapshot(
    String deviceId,
    long timestamp,
    String status,
    Double temperatureC,
    Double averageTemperatureC,
    Double humidityPct,
    Double co2Ppm,
    Double pm25,
    Double energyUsageW,
    boolean anomaly,
    Double anomalyScore,
    boolean leak,
    String krakowZone
) {
}
