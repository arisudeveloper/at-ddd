package br.com.freela.reputacao;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReputacaoEventListener {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoEventListener.class);
    private final ReputacaoService reputacaoService;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .findAndRegisterModules()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public ReputacaoEventListener(ReputacaoService reputacaoService) {
        this.reputacaoService = reputacaoService;
    }

    @KafkaListener(topics = "freela.contrato.eventos", groupId = "reputacao-group")
    public void consumir(String payload) {
        ContratoEventoDto evento;
        try {
            evento = objectMapper.readValue(payload, ContratoEventoDto.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Evento invalido: " + payload, e);
        }

        try (MDC.MDCCloseable ignored = MDC.putCloseable("correlationId", String.valueOf(evento.correlationId()))) {
            log.info("reputacao.consumo.inicio eventId={} contratoId={} eventType={}",
                    evento.eventId(), evento.contratoId(), evento.eventType());

            if ("ContratoConcluido".equalsIgnoreCase(evento.eventType()) || "CONTRATO_CONCLUIDO".equalsIgnoreCase(evento.eventType())) {
                reputacaoService.registrarContratoConcluido(
                        evento.eventId(),
                        evento.contratoId(),
                        evento.freelancerId(),
                        evento.valor()
                );
                log.info("reputacao.consumo.sucesso eventId={} contratoId={}", evento.eventId(), evento.contratoId());
            } else {
                log.info("reputacao.consumo.ignorado eventId={} eventType={}", evento.eventId(), evento.eventType());
            }
        }
    }
}