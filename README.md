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

## Recuperação manual de tarefas

Quando a política de tentativas é esgotada, `deadLetterIds()` lista uma cópia imutável dos identificadores das tarefas com falha, na ordem em que entraram na fila de erros. A aplicação pode tentar recuperá-las explicitamente com `replayDeadLetter(id)`:

```java
TaskQueue queue = new TaskQueue(2);
queue.submit("job-42", "dados");
queue.processNext(payload -> false);  // RETRYING
queue.processNext(payload -> false);  // DEAD_LETTER
System.out.println(queue.deadLetterIds()); // [job-42]
boolean requeued = queue.replayDeadLetter("job-42"); // true
queue.processNext(payload -> true);  // SUCCESS
```

A operação busca o **primeiro** registro com o ID informado (IDs repetidos são permitidos), remove-o da fila de erros, recoloca-o no fim da fila pronta e reinicia o contador de tentativas. Retorna `false` quando não encontra o ID. Esta função é **manual**, não realiza reprocessamentos contínuos e não executa código do worker sob o bloqueio da fila. Antes de repetir uma tarefa em um sistema real, avalie idempotência e efeitos externos.

## Segurança e limitações

Não há persistência, timeout imposto ao worker, retries com backoff ou entrega entre processos. Workers executam código do chamador e podem bloquear indefinidamente; não use código não confiável. A fila é apenas uma demonstração local.

## Evolução planejada

Trocar armazenamento por RabbitMQ e PostgreSQL, adicionar workers separados, idempotência, retry automático com backoff e métricas.

---

Projeto de Engenharia de Software mantido na conta [juniordev830](https://github.com/juniordev830).
