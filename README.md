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


## Fila confiável (API adicional)

A classe original `TaskQueue` permanece compatível. Para novos consumidores,
`ReliableTaskQueue` acrescenta controle de capacidade, prioridade, agendamento,
expiração por TTL, identificadores idempotentes, tentativas com backoff exponencial,
leases com tokens exclusivos, confirmação, cancelamento e reprocessamento manual de
dead letters. O relógio é injetável para testes determinísticos.

```java
import dev.juniordev.ReliableTaskQueue;
import dev.juniordev.TaskQueueWorker;
import dev.juniordev.TaskQueueReports;
import java.time.Clock;
import java.time.Duration;

var settings = new ReliableTaskQueue.Config(
    1000, 3, Duration.ofSeconds(30), Duration.ofSeconds(1),
    Duration.ofMinutes(2), 4096, 1000);
var queue = new ReliableTaskQueue(settings, Clock.systemUTC());

queue.submit(new ReliableTaskQueue.Request(
    "email-42", "enviar-notificacao", 10, Duration.ZERO, Duration.ofMinutes(5)));
var attempt = TaskQueueWorker.processOne(queue, payload -> {
    System.out.println("Processando " + payload);
});
System.out.println(attempt.outcome());
System.out.println(TaskQueueReports.prometheus(queue.stats()));
```

### Semântica das operações

| Operação | Efeito |
| --- | --- |
| `submit(request)` | Insere uma tarefa válida se o ID ainda não existe; rejeita capacidade excedida. |
| `claim()` | Seleciona a maior prioridade disponível, depois FIFO, e cria lease com token. |
| `acknowledge(id, token)` | Finaliza somente se o token ainda é válido. |
| `fail(id, token, reason)` | Agenda retry exponencial ou envia para dead letter. |
| `extendLease(id, token, duration)` | Estende o lease enquanto ele estiver válido. |
| `cancel(id)` | Cancela uma tarefa pronta ou em execução e invalida o token. |
| `replayDeadLetter(id)` | Reabre uma tarefa morta, reiniciando suas tentativas. |
| `sweep()` | Expira TTLs e leases vencidos; também é executado nas operações da fila. |
| `purgeTerminal(duration)` | Remove registros terminais antigos e libera seus IDs para reutilização. |
| `claimBatch(limit)` | Reserva várias tarefas em uma operação. |
| `list()`, `stats()`, `history()` | Fornecem snapshots imutáveis para diagnóstico. |

O backoff é limitado por `maxBackoff` e não cria threads de espera.
Tarefas atrasadas ficam disponíveis quando um consumidor chama `claim`.
A expiração de leases é detectada em `sweep` ou na próxima operação; não
há processo em segundo plano. Um worker demorado pode continuar executando
depois de perder seu lease: seu ACK será recusado, mas efeitos externos
precisam ser idempotentes no sistema consumidor.

`TaskQueueWorker.drain` executa um lote sequencial; `parallel` usa um
`ExecutorService` fornecido pelo chamador. Nenhum handler é executado sob
o lock interno da fila. `TaskQueueReports` exporta CSV com proteção contra
fórmulas de planilha, JSON com escaping, trilha de auditoria e métricas
compatíveis com Prometheus.

### Limitações e uso seguro

- **Não é uma fila distribuída:** não há RabbitMQ, persistência, transações,
  replicação, entrega entre processos ou recuperação após reinicialização.
- A semântica é **at-least-once** durante retries/expiração de leases. Use
  idempotência nas integrações externas; não confie em execução exatamente uma vez.
- O limite de capacidade conta apenas tarefas ativas. Registros terminais podem
  ser removidos por `purgeTerminal`; configure uma política de retenção.
- O histórico é circular e limitado por `historyLimit`; não substitui
  armazenamento permanente de auditoria.
- Não armazene segredos nos payloads: os relatórios incluem seus valores.
  O serviço não fornece autenticação nem autorização de operadores.
- O runner não interrompe automaticamente callbacks bloqueados; controle
  timeouts de rede e recursos dentro do handler.
- TTL e atraso são calculados em milissegundos a partir do relógio injetado.
  Saltos de relógio podem afetar a programação.
- Para produção, utilize broker durável, monitoramento, controle de acesso,
  armazenamento seguro e testes de recuperação sob falhas reais.

### Testes da expansão

```bash
mvn -B -ntp test
```

A suíte cobre retries, limites, TTL, prioridade, ordenação, concorrência,
expiração e renovação de leases, replay de dead letters, snapshots,
métricas, exportações CSV/JSON e segurança de dados de relatórios.
