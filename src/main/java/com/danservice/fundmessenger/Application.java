package com.danservice.fundmessenger;

import com.danservice.fundmessenger.adapter.outbound.kafka.streetorderack.v1.dto.KafkaStreetOrderAckDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@Slf4j
@EnableKafka
@SpringBootApplication
@RegisterReflectionForBinding(KafkaStreetOrderAckDTO.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication
                .run(Application.class, args);
    }

}
