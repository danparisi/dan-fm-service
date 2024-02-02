package com.danservice.fundmessenger.adapter.outbound.kafka.v1;

import com.danservice.fundmessenger.adapter.outbound.kafka.v1.dto.KafkaStreetOrderAckDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaProducer {
    @Value("${dan.topic.street-order-ack}")
    private String streetOrderAcksTopic;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendStreetOrderAck(KafkaStreetOrderAckDTO orderDTO) {
        String key = orderDTO.getId().toString();
        kafkaTemplate.send(streetOrderAcksTopic, key, orderDTO);
    }
}
