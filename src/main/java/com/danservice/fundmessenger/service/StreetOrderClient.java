package com.danservice.fundmessenger.service;

import com.danservice.fundmessenger.adapter.inbound.kafka.streetorder.v1.dto.KafkaStreetOrderDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

import static java.lang.Thread.sleep;
import static java.util.UUID.randomUUID;

@Slf4j
@Service
public class StreetOrderClient {

    //@SneakyThrows
    public UUID processStreetOrder(KafkaStreetOrderDTO streetOrderDTO) {
        try {
            log.info("Processing street order id=[{}]", streetOrderDTO.getId());
            sleep(new Random().nextInt(500, 1000));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return randomUUID();
    }
}
