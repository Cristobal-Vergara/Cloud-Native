package com.example.AndesStayKafkaAdmin.controller;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping({"/api/v1/kafka", "/api/kafka"})
public class KafkaAdminController {

    private final AdminClient adminClient;

    private static final Set<String> ALLOWED_CONFIGS = Set.of(
            "retention.ms",
            "retention.bytes",
            "cleanup.policy",
            "max.message.bytes",
            "segment.bytes",
            "min.insync.replicas"
    );

    public KafkaAdminController(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @GetMapping("/status")
    public String status() {
        return "Kafka Admin funcionando";
    }

    // LISTAR TOPICOS
    @GetMapping("/topics")
    public Collection<String> topics() {
        return await(adminClient.listTopics().names());
    }

    // CONSULTAR UN TOPICO
    @GetMapping("/topic/{topicName}")
    public Map<String, Object> topic(@PathVariable String topicName) {

        validateTopicName(topicName);

        TopicDescription description = await(
                adminClient.describeTopics(List.of(topicName))
                        .allTopicNames()
        ).get(topicName);

        if (description == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "No se encontró el tópico"
            );
        }

        Map<String, Object> result = new HashMap<>();
        result.put("name", description.name());
        result.put("partitions", description.partitions().size());
        result.put(
                "replicationFactor",
                description.partitions().isEmpty()
                        ? 0
                        : description.partitions().get(0).replicas().size()
        );

        return result;
    }

    // CREAR TOPICO
    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(
            @RequestBody CreateTopicRequest request) {

        if (request == null) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Se requiere el cuerpo de la solicitud"
            );
        }

        validateTopicName(request.name());

        int partitions = request.partitions() == null ? 1 : request.partitions();
        short replicationFactor = request.replicationFactor() == null
                ? 1
                : request.replicationFactor();

        if (partitions < 1) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Las particiones deben ser mayores que cero"
            );
        }

        if (replicationFactor < 1) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "El factor de replicación debe ser mayor que cero"
            );
        }

        Map<String, String> configs = validateConfigs(request.configs());

        NewTopic newTopic = new NewTopic(
                request.name(), partitions, replicationFactor
        );

        if (!configs.isEmpty()) {
            newTopic.configs(configs);
        }

        await(adminClient.createTopics(List.of(newTopic)).all());

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Tópico creado correctamente");
        response.put("name", request.name());
        response.put("partitions", partitions);
        response.put("replicationFactor", replicationFactor);

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    // MODIFICAR CONFIGURACION DE UN TOPICO
    @PutMapping("/topics/{topicName}/config")
    public Map<String, Object> updateTopicConfig(
            @PathVariable String topicName,
            @RequestBody UpdateTopicConfigRequest request) {

        validateTopicName(topicName);

        if (request == null || request.configs() == null
                || request.configs().isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Debes indicar al menos una configuración"
            );
        }

        Map<String, String> configs = validateConfigs(request.configs());

        ConfigResource resource = new ConfigResource(
                ConfigResource.Type.TOPIC, topicName
        );

        List<AlterConfigOp> operations = configs.entrySet().stream()
                .map(entry -> new AlterConfigOp(
                        new ConfigEntry(entry.getKey(), entry.getValue()),
                        AlterConfigOp.OpType.SET
                ))
                .toList();

        await(adminClient.incrementalAlterConfigs(
                Map.of(resource, operations)
        ).all());

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Configuración modificada correctamente");
        response.put("topic", topicName);
        response.put("configs", configs);

        return response;
    }

    // ELIMINAR TOPICO
    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topicName) {

        validateTopicName(topicName);

        await(adminClient.deleteTopics(List.of(topicName)).all());

        return ResponseEntity.noContent().build();
    }

    // VALIDAR NOMBRE DEL TOPICO
    private void validateTopicName(String name) {
        if (name == null || name.isBlank()
                || name.length() > 249
                || !name.matches("[a-zA-Z0-9._-]+")
                || name.equals(".")
                || name.equals("..")) {

            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Nombre de tópico inválido"
            );
        }
    }

    // VALIDAR CONFIGURACIONES PERMITIDAS
    private Map<String, String> validateConfigs(Map<String, String> configs) {

        if (configs == null || configs.isEmpty()) {
            return Map.of();
        }

        Map<String, String> validated = new HashMap<>();

        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!ALLOWED_CONFIGS.contains(key)) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Configuración no permitida: " + key
                );
            }

            if (value == null || value.isBlank()) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "El valor de " + key + " no puede estar vacío"
                );
            }

            if (key.equals("retention.ms") || key.equals("retention.bytes")) {
                validateNumber(key, value, -1);
            }

            if (key.equals("max.message.bytes")
                    || key.equals("segment.bytes")
                    || key.equals("min.insync.replicas")) {
                validateNumber(key, value, 1);
            }

            if (key.equals("cleanup.policy")
                    && !Set.of("delete", "compact", "delete,compact",
                               "compact,delete").contains(value)) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "cleanup.policy debe ser delete, compact o ambas"
                );
            }

            validated.put(key, value);
        }

        return validated;
    }

    private void validateNumber(String key, String value, long minimum) {
        try {
            if (Long.parseLong(value) < minimum) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Valor inválido para " + key
                );
            }
        } catch (NumberFormatException ex) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "El valor de " + key + " debe ser numérico"
            );
        }
    }

    // MANEJO DE RESPUESTAS ASINCRONAS DE KAFKA
    private <T> T await(KafkaFuture<T> future) {

        try {
            return future.get();

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "La operación con Kafka fue interrumpida"
            );

        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();

            if (cause instanceof TopicExistsException) {
                throw new ResponseStatusException(
                        HttpStatus.CONFLICT, "El tópico ya existe"
                );
            }

            if (cause instanceof UnknownTopicOrPartitionException) {
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "El tópico no existe"
                );
            }

            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Error al ejecutar la operación en Kafka: "
                            + (cause == null ? ex.getMessage() : cause.getMessage())
            );
        }
    }

    public record CreateTopicRequest(
            String name,
            Integer partitions,
            Short replicationFactor,
            Map<String, String> configs
    ) {}

    public record UpdateTopicConfigRequest(
            Map<String, String> configs
    ) {}
}
