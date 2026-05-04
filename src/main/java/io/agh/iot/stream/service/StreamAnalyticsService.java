package io.agh.iot.stream.service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.agh.iot.stream.config.StreamProperties;
import io.agh.iot.stream.model.DeviceStatusEvent;
import io.agh.iot.stream.model.ProcessedSnapshot;
import io.agh.iot.stream.model.TelemetryEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class StreamAnalyticsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamAnalyticsService.class);

    private final ObjectMapper objectMapper;
    private final StreamProperties streamProperties;
    private final RestClient alertApiRestClient;

    private final Map<String, String> statusByDevice = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>> tempWindowByDevice = new ConcurrentHashMap<>();
    private final Map<String, Deque<ProcessedSnapshot>> recentByDevice = new ConcurrentHashMap<>();
    private final Map<String, ProcessedSnapshot> latestByDevice = new ConcurrentHashMap<>();
    private final AtomicLong processedEvents = new AtomicLong();
    private final ScheduledExecutorService mqttReconnectExecutor = Executors.newSingleThreadScheduledExecutor();

    private MqttClient mqttClient;

    public StreamAnalyticsService(ObjectMapper objectMapper,
                                  StreamProperties streamProperties,
                                  RestClient alertApiRestClient) {
        this.objectMapper = objectMapper;
        this.streamProperties = streamProperties;
        this.alertApiRestClient = alertApiRestClient;
    }

    @PostConstruct
    public void init() {
        scheduleConnect(0);
    }

    @PreDestroy
    public void shutdown() {
        mqttReconnectExecutor.shutdownNow();
        if (mqttClient == null) {
            return;
        }
        try {
            mqttClient.disconnect();
            mqttClient.close();
        } catch (MqttException exception) {
            LOGGER.warn("Failed to close MQTT client: {}", exception.getMessage());
        }
    }

    public List<ProcessedSnapshot> getLatestSnapshots() {
        return new ArrayList<>(latestByDevice.values());
    }

    public List<ProcessedSnapshot> getSeriesForDevice(String deviceId) {
        return new ArrayList<>(recentByDevice.getOrDefault(deviceId, new ArrayDeque<>()));
    }

    public int getEventsPerMinuteEstimate() {
        return latestByDevice.size() * 12;
    }

    private void scheduleConnect(long delaySeconds) {
        mqttReconnectExecutor.schedule(this::connect, delaySeconds, TimeUnit.SECONDS);
    }

    private void connect() {
        if (mqttClient != null && mqttClient.isConnected()) {
            return;
        }

        try {
            mqttClient = new MqttClient(streamProperties.getMqttBrokerUri(), MqttClient.generateClientId());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            mqttClient.connect(options);
            LOGGER.info("stream.mqtt.connected broker={}", streamProperties.getMqttBrokerUri());
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    LOGGER.warn("MQTT connection lost: {}", cause != null ? cause.getMessage() : "unknown");
                    scheduleConnect(10);
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    process(topic, message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // no-op
                }
            });
            mqttClient.subscribe("devices/+/telemetry", 1);
            mqttClient.subscribe("devices/+/status", 1);
            LOGGER.info("stream.mqtt.subscribed topics=devices/+/telemetry,devices/+/status");
        } catch (MqttException exception) {
            LOGGER.warn("Unable to connect stream-worker to MQTT broker at {}: {}", streamProperties.getMqttBrokerUri(), exception.getMessage());
            scheduleConnect(15);
        }
    }

    private void process(String topic, MqttMessage message) {
        try {
            if (topic.endsWith("/status")) {
                DeviceStatusEvent event = objectMapper.readValue(message.getPayload(), DeviceStatusEvent.class);
                statusByDevice.put(event.deviceId(), event.status());
                LOGGER.info("stream.status device={} status={} battery={} zone={}", event.deviceId(), event.status(), event.batteryPct(), event.krakowZone());
                return;
            }

            TelemetryEvent event = objectMapper.readValue(message.getPayload(), TelemetryEvent.class);
            ProcessedSnapshot snapshot = toSnapshot(event);
            latestByDevice.put(snapshot.deviceId(), snapshot);
            long count = processedEvents.incrementAndGet();

            Deque<ProcessedSnapshot> series = recentByDevice.computeIfAbsent(snapshot.deviceId(), key -> new ArrayDeque<>());
            series.addLast(snapshot);
            while (series.size() > 120) {
                series.removeFirst();
            }

            LOGGER.info(
                "stream.process device={} temp={} avgTemp={} humidity={} energy={} anomaly={} score={} leak={} count={}",
                snapshot.deviceId(),
                snapshot.temperatureC(),
                snapshot.averageTemperatureC(),
                snapshot.humidityPct(),
                snapshot.energyUsageW(),
                snapshot.anomaly(),
                snapshot.anomalyScore(),
                snapshot.leak(),
                count
            );

            if (snapshot.anomaly()) {
                LOGGER.warn("stream.anomaly device={} temp={} avgTemp={} score={}", snapshot.deviceId(), snapshot.temperatureC(), snapshot.averageTemperatureC(), snapshot.anomalyScore());
            }

            alertApiRestClient.post()
                .uri("/evaluate")
                .body(Map.of(
                    "deviceId", snapshot.deviceId(),
                    "temperature", snapshot.temperatureC(),
                    "threshold", 30.0,
                    "leak", snapshot.leak(),
                    "anomaly", snapshot.anomaly(),
                    "anomalyScore", snapshot.anomalyScore(),
                    "krakowZone", snapshot.krakowZone()
                ))
                .retrieve()
                .toBodilessEntity();
            LOGGER.info("stream.forward.alert device={} temp={} leak={} anomaly={}", snapshot.deviceId(), snapshot.temperatureC(), snapshot.leak(), snapshot.anomaly());
        } catch (Exception exception) {
            LOGGER.warn("Ignoring malformed MQTT payload on {}: {}", topic, exception.getMessage());
        }
    }

    private ProcessedSnapshot toSnapshot(TelemetryEvent event) {
        Deque<Double> tempWindow = tempWindowByDevice.computeIfAbsent(event.deviceId(), key -> new ArrayDeque<>());
        if (event.temperatureC() != null) {
            tempWindow.addLast(event.temperatureC());
        }
        while (tempWindow.size() > 30) {
            tempWindow.removeFirst();
        }

        double avgTemp = tempWindow.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double stdDev = calculateStdDev(tempWindow, avgTemp);
        double score = (event.temperatureC() == null || stdDev < 0.001) ? 0.0 : Math.abs(event.temperatureC() - avgTemp) / stdDev;
        boolean anomaly = score >= 2.5;

        String currentStatus = statusByDevice.getOrDefault(event.deviceId(), "ONLINE");

        return new ProcessedSnapshot(
            event.deviceId(),
            event.timestamp(),
            currentStatus,
            event.temperatureC(),
            round(avgTemp),
            event.humidityPct(),
            event.co2Ppm(),
            event.pm25(),
            event.energyUsageW(),
            anomaly,
            round(score),
            event.leak(),
            event.krakowZone()
        );
    }

    private static double calculateStdDev(Deque<Double> values, double avg) {
        if (values.isEmpty()) {
            return 0.0;
        }
        double variance = values.stream()
            .mapToDouble(value -> Math.pow(value - avg, 2))
            .average()
            .orElse(0.0);
        return Math.sqrt(variance);
    }

    private static Double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
