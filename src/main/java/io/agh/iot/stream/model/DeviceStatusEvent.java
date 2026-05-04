package io.agh.iot.stream.model;

public record DeviceStatusEvent(
    String deviceId,
    long timestamp,
    String status,
    int batteryPct,
    String krakowZone
) {
}
