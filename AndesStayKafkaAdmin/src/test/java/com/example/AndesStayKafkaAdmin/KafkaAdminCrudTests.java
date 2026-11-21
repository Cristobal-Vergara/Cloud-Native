package com.example.AndesStayKafkaAdmin;

import com.example.AndesStayKafkaAdmin.config.SecurityConfig;
import com.example.AndesStayKafkaAdmin.controller.KafkaAdminController;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Set;
import java.util.Map;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaAdminController.class)
@Import(SecurityConfig.class)
class KafkaAdminCrudTests {

    @Autowired
    MockMvc mvc;

    @MockitoBean
    JwtDecoder decoder;

    @MockitoBean
    AdminClient adminClient;

    @BeforeEach
    void authenticateAdmin() {
        when(decoder.decode("test-token")).thenReturn(
                Jwt.withTokenValue("test-token")
                        .header("alg", "RS256")
                        .subject("admin-test")
                        .claim("roles", Set.of("Admin"))
                        .build()
        );
    }

    @Test
    void createTopicSuccessfully() throws Exception {
        CreateTopicsResult result = mock(CreateTopicsResult.class);

        when(adminClient.createTopics(anyCollection())).thenReturn(result);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));

        mvc.perform(post("/api/v1/kafka/topics")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                        "name": "andesstay.test",
                        "partitions": 1,
                        "replicationFactor": 1
                    }
                    """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("andesstay.test"))
                .andExpect(jsonPath("$.partitions").value(1))
                .andExpect(jsonPath("$.replicationFactor").value(1));

        verify(adminClient).createTopics(anyCollection());
    }

    @Test
    void listTopicsSuccessfully() throws Exception {
        ListTopicsResult result = mock(ListTopicsResult.class);

        when(adminClient.listTopics()).thenReturn(result);
        when(result.names()).thenReturn(
                KafkaFuture.completedFuture(Set.of(
                        "andesstay.events",
                        "andesstay.test"
                ))
        );

        mvc.perform(get("/api/v1/kafka/topics")
                .header("Authorization", "Bearer test-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0]").exists())
                .andExpect(jsonPath("$[1]").exists());

        verify(adminClient).listTopics();
    }

    @Test
    void deleteTopicSuccessfully() throws Exception {
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);

        when(adminClient.deleteTopics(anyCollection())).thenReturn(result);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));

        mvc.perform(delete("/api/v1/kafka/topics/andesstay.test")
                .header("Authorization", "Bearer test-token"))
                .andExpect(status().isNoContent());

        verify(adminClient).deleteTopics(anyCollection());
    }

    @Test
    void rejectInvalidTopicName() throws Exception {
        mvc.perform(post("/api/v1/kafka/topics")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                        "name": "nombre invalido!",
                        "partitions": 1,
                        "replicationFactor": 1
                    }
                    """))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(adminClient);
    }

    @Test
    void getTopicDetailsSuccessfully() throws Exception {
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);

        Node broker = new Node(1, "localhost", 9092);
        TopicPartitionInfo partition = new TopicPartitionInfo(
                0, broker, List.of(broker), List.of(broker)
        );

        TopicDescription description = new TopicDescription(
                "andesstay.test", false, List.of(partition)
        );

        when(adminClient.describeTopics(anyCollection())).thenReturn(result);
        when(result.allTopicNames()).thenReturn(
                KafkaFuture.completedFuture(
                        Map.of("andesstay.test", description)
                )
        );

        mvc.perform(get("/api/v1/kafka/topic/andesstay.test")
                .header("Authorization", "Bearer test-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("andesstay.test"))
                .andExpect(jsonPath("$.partitions").value(1))
                .andExpect(jsonPath("$.replicationFactor").value(1));

        verify(adminClient).describeTopics(anyCollection());
    }

    @Test
    void updateTopicConfigSuccessfully() throws Exception {
        AlterConfigsResult result = mock(AlterConfigsResult.class);

        when(adminClient.incrementalAlterConfigs(anyMap())).thenReturn(result);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));

        mvc.perform(put("/api/v1/kafka/topics/andesstay.test/config")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                        "configs": {
                            "retention.ms": "86400000"
                        }
                    }
                    """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topic").value("andesstay.test"))
                .andExpect(jsonPath("$.configs['retention.ms']").value("86400000"));

        verify(adminClient).incrementalAlterConfigs(anyMap());
    }

    @Test
    void rejectUnsupportedTopicConfig() throws Exception {
        mvc.perform(put("/api/v1/kafka/topics/andesstay.test/config")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                        "configs": {
                            "configuracion.invalida": "valor"
                        }
                    }
                    """))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(adminClient);
    }
}
