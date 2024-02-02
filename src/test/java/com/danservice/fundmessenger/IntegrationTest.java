package com.danservice.fundmessenger;

import com.danservice.fundmessenger.adapter.inbound.kafka.v1.dto.KafkaStreetOrderDTO;
import com.danservice.fundmessenger.adapter.outbound.kafka.v1.dto.KafkaStreetOrderAckDTO;
import com.danservice.fundmessenger.service.StreetOrderClient;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.danservice.fundmessenger.domain.OrderType.LIMIT;
import static java.lang.Thread.sleep;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.UUID.randomUUID;
import static org.apache.commons.collections4.IterableUtils.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;

@SpringBootTest(classes = Application.class)
@EmbeddedKafka(partitions = 1, topics = {"${dan.topic.street-order-ack}"})
class IntegrationTest {
    private static final UUID A_STREET_ID = randomUUID();
    private static final EasyRandom EASY_RANDOM = new EasyRandom();


    @Value("${dan.topic.street-order}")
    private String streetOrdersTopic;
    @Value("${dan.topic.street-order-ack}")
    private String streetOrderAcksTopic;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockBean
    private StreetOrderClient streetOrderClient;

    @Test
    @SneakyThrows
    void shouldHandleClientOrder() {
        KafkaStreetOrderDTO streetOrderDTO = aKafkaStreetOrderDTO();
        when(streetOrderClient.processStreetOrder(streetOrderDTO)).thenReturn(A_STREET_ID);

        kafkaTemplate.send(streetOrdersTopic, streetOrderDTO.getId().toString(), streetOrderDTO).get();

        verifyKafkaStreetOrderProduced(streetOrderDTO);
    }

    private static KafkaStreetOrderDTO aKafkaStreetOrderDTO() {
        return KafkaStreetOrderDTO.builder()
                .type(LIMIT)
                .id(randomUUID())
                .instrument(randomAlphabetic(15))
                .quantity(EASY_RANDOM.nextInt(1, 100))
                .price(BigDecimal.valueOf(EASY_RANDOM.nextDouble(1.0d, 100.0d))).build();
    }

    private void verifyKafkaStreetOrderProduced(KafkaStreetOrderDTO streetOrderDTO) {
        List<ConsumerRecord<String, KafkaStreetOrderAckDTO>> consumerRecords = consumeFromKafkaStreetOrderAckTopic();

        assertEquals(1, consumerRecords.size());
        KafkaStreetOrderAckDTO actual = consumerRecords.get(0).value();

        assertEquals(A_STREET_ID, actual.getStreetId());
        assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFields("streetId")
                .isEqualTo(streetOrderDTO);
    }

    private List<ConsumerRecord<String, KafkaStreetOrderAckDTO>> consumeFromKafkaStreetOrderAckTopic() {
        Map<String, Object> consumerProps = consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(TRUSTED_PACKAGES, "com.danservice.*");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        ConsumerFactory<String, KafkaStreetOrderAckDTO> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<String, KafkaStreetOrderAckDTO> consumer = cf.createConsumer();

        embeddedKafkaBroker
                .consumeFromAnEmbeddedTopic(consumer, streetOrderAcksTopic);

        return toList(
                getRecords(consumer, Duration.of(30, SECONDS))
                        .records(streetOrderAcksTopic));
    }
}
