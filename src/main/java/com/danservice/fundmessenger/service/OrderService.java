package com.danservice.fundmessenger.service;

import com.danservice.fundmessenger.adapter.KafkaStreetOrderMapper;
import com.danservice.fundmessenger.adapter.inbound.kafka.streetorder.v1.dto.KafkaStreetOrderDTO;
import com.danservice.fundmessenger.adapter.outbound.kafka.streetorderack.v1.KafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final KafkaProducer kafkaProducer;
    private final StreetOrderClient streetOrderClient;
    private final KafkaStreetOrderMapper kafkaStreetOrderMapper;

    @SneakyThrows
    public void handleStreetOrder(KafkaStreetOrderDTO streetOrderDTO) {
        UUID streetId = streetOrderClient.processStreetOrder(streetOrderDTO);
        log.info("Processed street order: id=[{}], streetId=[{}]", streetOrderDTO.getId(), streetId);

        kafkaProducer.sendStreetOrderAck(
                kafkaStreetOrderMapper.map(streetOrderDTO, streetId));
    }
}
