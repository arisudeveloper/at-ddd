package br.com.freela.contrato.infrastructure.kafka;

import br.com.freela.contrato.domain.event.ContratoEventoDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventoPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventoPublisher.class);
    private final KafkaTemplate<String, ContratoEventoDto> kafkaTemplate;

    public KafkaEventoPublisher(KafkaTemplate<String, ContratoEventoDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publicar(String topico, ContratoEventoDto evento) {
        log.info("kafka.publicacao.inicio topico={} eventId={} contratoId={}", topico, evento.eventId(), evento.contratoId());
        kafkaTemplate.send(topico, evento.contratoId().toString(), evento);
        log.info("kafka.publicacao.sucesso topico={} eventId={} contratoId={}", topico, evento.eventId(), evento.contratoId());
    }
}