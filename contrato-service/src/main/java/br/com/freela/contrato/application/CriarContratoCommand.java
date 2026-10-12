package br.com.freela.contrato.application;

import java.math.BigDecimal;
import java.util.UUID;

public record CriarContratoCommand(
        UUID clienteId,
        UUID freelancerId,
        String titulo,
        BigDecimal valor,
        String correlationId
) {
    public CriarContratoCommand(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor) {
        this(clienteId, freelancerId, titulo, valor, null);
    }
}