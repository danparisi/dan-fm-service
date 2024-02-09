package com.danservice.fundmessenger.adapter;


import com.danservice.fundmessenger.adapter.inbound.kafka.streetorder.v1.dto.KafkaStreetOrderDTO;
import com.danservice.fundmessenger.adapter.outbound.kafka.streetorderack.v1.dto.KafkaStreetOrderAckDTO;
import org.mapstruct.Mapper;

import java.util.UUID;

@Mapper(componentModel = "spring")
public interface KafkaStreetOrderMapper {

    KafkaStreetOrderAckDTO map(KafkaStreetOrderDTO apiOrderDTO, UUID streetId);

}
