
package com.danservice.fundmessenger.adapter.outbound.kafka.orderexecution.v1;


import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class KafkaOrderExecutionProducer {
    @Value("${dan.topic.street-order-execution}")
    private String streetOrderAcksTopic;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendOrderExecution(UUID orderId) {
        kafkaTemplate.send(streetOrderAcksTopic, orderId.toString(), orderId);
    }
}
