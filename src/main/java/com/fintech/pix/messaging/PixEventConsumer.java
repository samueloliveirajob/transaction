package com.fintech.pix.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.pix.observability.Mdc;
import com.fintech.pix.worker.PixProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile({"worker", "all"})
@RequiredArgsConstructor
@Slf4j
public class PixEventConsumer {

    private final PixProcessingService processingService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = Topics.PIX_REQUESTED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPixRequested(String payload) throws Exception {
        PixRequestedEvent event = objectMapper.readValue(payload, PixRequestedEvent.class);
        Mdc.run(event.transactionId(), () -> {
            log.info("processing transactionId={}", event.transactionId());
            processingService.process(event);
        });
    }
}
