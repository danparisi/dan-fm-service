package com.danservice.fundmessenger.service;

import com.danservice.fundmessenger.adapter.inbound.kafka.streetorder.v1.dto.KafkaStreetOrderDTO;
import com.danservice.fundmessenger.adapter.outbound.kafka.internalexecution.v1.KafkaInternalOrderExecutionProducer;
import com.danservice.fundmessenger.adapter.outbound.kafka.orderexecution.v1.KafkaOrderExecutionProducer;
import com.danservice.fundmessenger.adapter.outbound.kafka.streetorderack.v1.KafkaStreetOrderAckProducer;
import com.danservice.fundmessenger.adapter.outbound.kafka.streetorderack.v1.KafkaStreetOrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

import static java.lang.Thread.sleep;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final StreetOrderClient streetOrderClient;
    private final KafkaStreetOrderMapper kafkaStreetOrderMapper;
    private final KafkaOrderExecutionProducer kafkaOrderExecutionProducer;
    private final KafkaStreetOrderAckProducer kafkaStreetOrderAckProducer;
    private final KafkaInternalOrderExecutionProducer kafkaInternalOrderExecutionProducer;

    @SneakyThrows
    public void handleStreetOrder(KafkaStreetOrderDTO streetOrderDTO) {
        UUID streetId = streetOrderClient.processStreetOrder(streetOrderDTO);
        log.info("Processed street order: id=[{}], streetId=[{}]", streetOrderDTO.getId(), streetId);

        kafkaStreetOrderAckProducer.sendStreetOrderAck(
                kafkaStreetOrderMapper.map(streetOrderDTO, streetId));
        kafkaInternalOrderExecutionProducer
                .sendInternalOrderExecution(streetOrderDTO.getId());
    }

    @SneakyThrows
    public void handleOrderExecution(UUID orderId) {
        log.info("Order Executed: id=[{}]", orderId);
        sleep(new Random().nextInt(100, 500));

        kafkaOrderExecutionProducer.sendOrderExecution(orderId);
    }
}
