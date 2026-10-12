package br.com.freela.contrato.infrastructure.kafka;

import br.com.freela.contrato.domain.event.ContratoEventoDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventoPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventoPublisher.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaEventoPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publicar(String topico, ContratoEventoDto evento) {
        log.info("kafka.publicacao.inicio topico={} eventId={} contratoId={}", topico, evento.eventId(), evento.contratoId());
        try {
            String jsonPayload = objectMapper.writeValueAsString(evento);
            kafkaTemplate.send(topico, evento.contratoId().toString(), jsonPayload);
            log.info("kafka.publicacao.sucesso topico={} eventId={} contratoId={}", topico, evento.eventId(), evento.contratoId());
        } catch (Exception e) {
            log.error("kafka.publicacao.erro topico={} eventId={}", topico, evento.eventId(), e);
            throw new RuntimeException("Erro ao converter/publicar evento no Kafka", e);
        }
    }
}