package com.example.AndesStay.service;

import org.springframework.amqp.core.DirectExchange;
import com.example.AndesStay.entity.OutboxEvent;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import tools.jackson.databind.json.JsonMapper;

@Component
public class BrokerGateway {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final RabbitTemplate rabbit;
    private final KafkaTemplate<String, String> kafka;
    private final long timeoutSeconds;
    private final DirectExchange exchange;
    private final Map<ReservationEventType, String> routingKeys;

    public BrokerGateway(RabbitTemplate rabbit, KafkaTemplate<String, String> kafka,
            @Value("${andesstay.outbox.confirm-timeout-seconds:10}") long timeoutSeconds,
            DirectExchange reservationExchange,
            @Qualifier("reservationRoutingKeys") Map<ReservationEventType, String> routingKeys) {
        this.rabbit = rabbit;
        this.kafka = kafka;
        this.timeoutSeconds = timeoutSeconds;
        this.exchange = reservationExchange;
        this.routingKeys = Map.copyOf(routingKeys);
    }

    public void publish(OutboxEvent event) throws Exception {
        if (event.getDestination().equals("KAFKA")) {
            String eventType = JSON.readTree(event.getPayload()).path("type").asString();

            String topic = switch (eventType) {
                case "RESERVATION_CREATED",
                     "RESERVATION_CONFIRMED",
                     "RESERVATION_CANCELLED" -> "andesstay.reservations";
                case "AUDIT_EVENT" -> "andesstay.audit";
                default -> throw new IllegalArgumentException(
                        "Tipo de evento Kafka no soportado: " + eventType);
            };

            kafka.send(topic, event.getId(), event.getPayload())
                    .get(timeoutSeconds, TimeUnit.SECONDS);
            return;
        }

        MessageProperties properties = new MessageProperties();
        properties.setMessageId(event.getId());
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setContentEncoding(StandardCharsets.UTF_8.name());
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);

        CorrelationData correlation = new CorrelationData(event.getId());
        String routingKey = routingKeys.get(
                ReservationEventType.fromPayload(event.getPayload()));

        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalStateException(
                    "El evento no tiene una ruta de entrega configurada");
        }

        rabbit.send(exchange.getName(), routingKey,
                new Message(event.getPayload().getBytes(StandardCharsets.UTF_8), properties),
                correlation);

        CorrelationData.Confirm confirm = correlation.getFuture()
                .get(timeoutSeconds, TimeUnit.SECONDS);

        if (!confirm.ack() || correlation.getReturned() != null) {
            throw new IllegalStateException(
                    "El broker no confirmó el mensaje o no existe su destino");
        }
    }
}