# Distributed Task Queue

Fila de tarefas em Java 17 com política de tentativas, estados `SUCCESS`, `RETRYING` e `DEAD_LETTER` e validação de entradas. **MVP em memória e em processo único**: ainda não implementa distribuição entre serviços.

## Estado

**MVP funcional / projeto inicial de portfólio.** Implementa o núcleo descrito abaixo, mas não é produto pronto para produção.

## Como executar e testar

```bash
mvn -B test
```

O GitHub Actions executa os testes automaticamente em pushes e pull requests.

## Concorrência e comportamento

- A fila protege suas estruturas internas com sincronização, mas executa o `worker` **fora do bloqueio**. Uma tarefa lenta não impede novas submissões ou consultas.
- `pending()` conta apenas tarefas aguardando processamento, não as tarefas em execução.
- Se uma tarefa falhar, ela é recolocada no fim da fila, ou enviada à fila de falhas quando esgota as tentativas.
- Com múltiplos consumidores, a ordem de **conclusão** pode ser diferente da ordem de entrada. Não há garantia de processamento exatamente uma vez.
- Os testes incluem concorrência de submissões durante execução lenta de uma tarefa.

## Segurança e limitações

Não há persistência, timeout imposto ao worker, retries com backoff ou entrega entre processos. Workers executam código do chamador e podem bloquear indefinidamente; não use código não confiável. A fila é apenas uma demonstração local.

## Evolução planejada

Trocar armazenamento por RabbitMQ e PostgreSQL, adicionar workers separados, idempotência, retry com backoff e métricas.

---

Projeto de Engenharia de Software mantido na conta [juniordev830](https://github.com/juniordev830).
