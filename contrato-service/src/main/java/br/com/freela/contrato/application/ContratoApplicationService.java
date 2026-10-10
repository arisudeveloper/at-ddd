package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.ContratoEventoDto;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.kafka.KafkaEventoPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class ContratoApplicationService {
    private static final String TOPICO_CONTRATOS = "freela.contrato.eventos";
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final KafkaEventoPublisher eventPublisher;

    public ContratoApplicationService(ContratoRepository repository, KafkaEventoPublisher eventPublisher) {
        this.repository = repository;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);
        publicarEventos(salvo);
        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        log.info("contrato.entrega.inicio contratoId={}", id);
        Contrato contrato = buscar(id);
        contrato.registrarEntrega();
        Contrato salvo = repository.salvar(contrato);
        publicarEventos(salvo);
        log.info("contrato.entrega.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato concluir(UUID id) {
        log.info("contrato.conclusao.inicio contratoId={}", id);
        Contrato contrato = buscar(id);
        contrato.concluir();
        Contrato salvo = repository.salvar(contrato);
        publicarEventos(salvo);
        log.info("contrato.conclusao.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }

    private void publicarEventos(Contrato contrato) {
        for (DomainEvent event : contrato.pullDomainEvents()) {
            if (event instanceof ContratoCriado e) {
                ContratoEventoDto dto = new ContratoEventoDto(
                        e.eventId(),
                        e.eventType(),
                        e.contratoId(),
                        e.clienteId(),
                        e.freelancerId(),
                        e.titulo(),
                        e.valor(),
                        e.occurredAt(),
                        UUID.randomUUID().toString()
                );
                eventPublisher.publicar(TOPICO_CONTRATOS, dto);
            } else if (event instanceof EntregaRegistrada e) {
                ContratoEventoDto dto = new ContratoEventoDto(
                        e.eventId(),
                        e.eventType(),
                        e.contratoId(),
                        contrato.clienteId(),
                        contrato.freelancerId(),
                        contrato.titulo(),
                        contrato.valor(),
                        e.occurredAt(),
                        UUID.randomUUID().toString()
                );
                eventPublisher.publicar(TOPICO_CONTRATOS, dto);
            } else if (event instanceof ContratoConcluido e) {
                ContratoEventoDto dto = new ContratoEventoDto(
                        e.eventId(),
                        e.eventType(),
                        e.contratoId(),
                        e.freelancerId(),
                        e.freelancerId(),
                        contrato.titulo(),
                        e.valor(),
                        e.occurredAt(),
                        UUID.randomUUID().toString()
                );
                eventPublisher.publicar(TOPICO_CONTRATOS, dto);
            }
        }
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }
}