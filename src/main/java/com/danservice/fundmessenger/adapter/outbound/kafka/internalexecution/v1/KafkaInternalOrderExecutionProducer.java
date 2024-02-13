
package com.danservice.fundmessenger.adapter.outbound.kafka.internalexecution.v1;


import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class KafkaInternalOrderExecutionProducer {
    @Value("${dan.topic.int-fund-messenger-execution}")
    private String streetOrderAcksTopic;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendInternalOrderExecution(UUID orderId) {
        kafkaTemplate.send(streetOrderAcksTopic, orderId.toString(), orderId);
    }
}
