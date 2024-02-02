package com.danservice.fundmessenger.adapter.inbound.kafka.v1;

import com.danservice.fundmessenger.adapter.inbound.kafka.v1.dto.KafkaStreetOrderDTO;
import com.danservice.fundmessenger.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import static org.springframework.kafka.support.KafkaHeaders.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreetOrderListener {
    private final OrderService orderService;

    @KafkaListener(id = "${dan.topic.street-order}", topics = "${dan.topic.street-order}")
    public void listen(@Payload KafkaStreetOrderDTO payload, @Header(name = RECEIVED_KEY) String key) {
        log.info("Received street order: Key=[{}], value=[{}]", key, payload);

        orderService.handleStreetOrder(payload);
    }
}
