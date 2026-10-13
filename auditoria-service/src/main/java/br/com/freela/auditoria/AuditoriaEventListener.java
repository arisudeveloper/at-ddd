package br.com.freela.auditoria;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.slf4j.MDC;

import java.util.UUID;

@Component
public class AuditoriaEventListener {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaEventListener.class);
    private final AuditoriaService auditoriaService;
    private final ObjectMapper objectMapper;

    public AuditoriaEventListener(AuditoriaService auditoriaService, ObjectMapper objectMapper) {
        this.auditoriaService = auditoriaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "freela.contrato.eventos", groupId = "auditoria-group-v8")
    public void consumir(String payloadJson) {
        try {
            JsonNode node = objectMapper.readTree(payloadJson);

            UUID eventId = node.has("eventId") ? UUID.fromString(node.get("eventId").asText()) : UUID.randomUUID();

            UUID aggregateId = UUID.randomUUID();
            if (node.has("contratoId")) {
                aggregateId = UUID.fromString(node.get("contratoId").asText());
            } else if (node.has("aggregateId")) {
                aggregateId = UUID.fromString(node.get("aggregateId").asText());
            } else if (node.has("id")) {
                aggregateId = UUID.fromString(node.get("id").asText());
            }

            String eventType = node.has("eventType") ? node.get("eventType").asText() : "CONTRATO_CRIADO";
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : UUID.randomUUID().toString();

            MDC.put("correlationId", correlationId);
            log.info(">>> EVENTO RECEBIDO DO KAFKA: {}", payloadJson);

            auditoriaService.registrar(eventId, aggregateId, eventType, correlationId, payloadJson);
            log.info(">>> AUDITORIA PERSISTIDA COM SUCESSO NO POSTGRESQL!");

        } catch (Exception e) {
            log.error("Erro ao processar evento de auditoria", e);
            auditoriaService.registrar(UUID.randomUUID(), UUID.randomUUID(), "EVENTO_BRUTO", UUID.randomUUID().toString(), payloadJson);
        } finally {
            MDC.remove("correlationId");
        }
    }
}