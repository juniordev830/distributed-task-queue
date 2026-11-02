# Distributed Task Queue

Fila Java 17 de processamento de trabalhos com política de tentativas, retorno `SUCCESS/RETRYING/DEAD_LETTER` e validação. **MVP em memória e em processo único**, não distribuído ainda.

## Estado

**MVP funcional / projeto inicial de portfólio.** Implementa o núcleo descrito abaixo, mas não é produto pronto para produção.

## Como executar

```bash
mvn -B test
```

## Testes

```bash
mvn -B test
```

O GitHub Actions executa os testes automaticamente em pushes e pull requests.

## Segurança e limitações

Operações sincronizadas; o worker é executado durante a seção crítica (apenas demonstração). Sem persistência, timeout, retries com backoff ou entrega entre processos. Não apresenta garantias de exatamente uma execução.

## Evolução planejada

Trocar armazenamento por RabbitMQ e PostgreSQL, adicionar workers separados, idempotência, retry com backoff e métricas.

---
Projeto de Engenharia de Software mantido na conta [juniordev830](https://github.com/juniordev830).
