package br.com.freela.reputacao;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ContratoEventoDto(
        UUID eventId,
        String eventType,
        UUID contratoId,
        UUID clienteId,
        UUID freelancerId,
        String titulo,
        BigDecimal valor,
        Instant occurredAt,
        String correlationId
) {}