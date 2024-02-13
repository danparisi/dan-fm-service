package com.danservice.fundmessenger.adapter.inbound.kafka.internalexecution.v1;

import com.danservice.fundmessenger.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.UUID;

import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY;

@Slf4j
@Service
@RequiredArgsConstructor
public class InternalOrderExecutionListener {
    private final OrderService orderService;

    @KafkaListener(id = "${dan.topic.int-fund-messenger-execution}", topics = "${dan.topic.int-fund-messenger-execution}")
    public void listen(@Payload UUID payload, @Header(name = RECEIVED_KEY) String key) {
        log.info("Received street order: Key=[{}], value=[{}]", key, payload);

        orderService.handleOrderExecution(payload);
    }
}
