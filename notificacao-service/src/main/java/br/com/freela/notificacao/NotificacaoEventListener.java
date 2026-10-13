package br.com.freela.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificacaoEventListener {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoEventListener.class);
    private final NotificacaoService notificacaoService;

    public NotificacaoEventListener(NotificacaoService notificacaoService) {
        this.notificacaoService = notificacaoService;
    }

    @KafkaListener(topics = "freela.contrato.eventos", groupId = "notificacao-group")
    public void consumir(ContratoEventoDto evento) {
        try (MDC.MDCCloseable ignored = MDC.putCloseable("correlationId", String.valueOf(evento.correlationId()))) {
            log.info("notificacao.consumo.inicio eventId={} contratoId={} eventType={}",
                    evento.eventId(), evento.contratoId(), evento.eventType());

            String mensagem = "Novo contrato criado: " + evento.titulo();
            notificacaoService.registrar(
                    evento.eventId(),
                    evento.contratoId(),
                    evento.clienteId(),
                    evento.eventType(),
                    mensagem
            );

            log.info("notificacao.consumo.sucesso eventId={} contratoId={}", evento.eventId(), evento.contratoId());
        }
    }
}