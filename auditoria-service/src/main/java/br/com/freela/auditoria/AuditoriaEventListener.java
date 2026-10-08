package br.com.freela.auditoria;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuditoriaEventListener {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaEventListener.class);
    private final AuditoriaService auditoriaService;
    private final ObjectMapper objectMapper;

    public AuditoriaEventListener(AuditoriaService auditoriaService, ObjectMapper objectMapper) {
        this.auditoriaService = auditoriaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "freela.contrato.eventos", groupId = "auditoria-group")
    public void consumir(ContratoEventoDto evento) {
        log.info("auditoria.consumo.inicio eventId={} aggregateId={} eventType={}",
                evento.eventId(), evento.contratoId(), evento.eventType());
        try {
            String payloadJson = objectMapper.writeValueAsString(evento);
            auditoriaService.registrar(
                    evento.eventId(),
                    evento.contratoId(),
                    evento.eventType(),
                    evento.correlationId(),
                    payloadJson
            );
            log.info("auditoria.consumo.sucesso eventId={} aggregateId={}", evento.eventId(), evento.contratoId());
        } catch (JsonProcessingException e) {
            log.error("auditoria.consumo.erro eventId={} erro={}", evento.eventId(), e.getMessage(), e);
        }
    }
}