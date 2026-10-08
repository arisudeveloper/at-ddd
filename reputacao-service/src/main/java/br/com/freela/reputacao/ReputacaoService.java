package br.com.freela.reputacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;

@Service
public class ReputacaoService {
 private static final Logger log = LoggerFactory.getLogger(ReputacaoService.class);
 private final ReputacaoRepository repository;
 private final EventoProcessadoRepository eventoProcessadoRepository;

 public ReputacaoService(ReputacaoRepository repository, EventoProcessadoRepository eventoProcessadoRepository) {
  this.repository = repository;
  this.eventoProcessadoRepository = eventoProcessadoRepository;
 }

 @Transactional
 public void registrarContratoConcluido(UUID eventId, UUID contratoId, UUID freelancerId, BigDecimal valor) {
  log.info("reputacao.atualizacao.inicio eventId={} contratoId={} freelancerId={} valor={}", eventId, contratoId, freelancerId, valor);

  if (eventoProcessadoRepository.existsById(eventId)) {
   log.warn("reputacao.atualizacao.duplicado.ignorado eventId={}", eventId);
   return;
  }

  var r = repository.findById(freelancerId).orElseGet(() -> new ReputacaoFreelancer(freelancerId));
  r.registrarContrato(valor);
  repository.save(r);
  eventoProcessadoRepository.save(new EventoProcessado(eventId));

  log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} contratosConcluidos={} valorTotal={}",
          eventId, contratoId, freelancerId, r.contratosConcluidos, r.valorTotal);
 }
}