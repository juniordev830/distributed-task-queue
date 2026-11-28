# Engenharia reversa e estudo — Distributed Task Queue

## Objetivo e fluxo

Processamento de tarefas com tentativas e acompanhamento de estado. ReliableTaskQueue mantém o ciclo de estado e as concessões, TaskQueueWorker aplica o processamento e TaskQueueReports consolida visões.

**Tecnologias identificadas** · Java.

**Revisão de referência** · `4b4e79a4c821`.

## Mapa do código

| Caminho | Papel de leitura | Símbolos selecionados |
| --- | --- | --- |
| [src/main/java/dev/juniordev/ReliableTaskQueue.java](../src/main/java/dev/juniordev/ReliableTaskQueue.java) | Implementação | `ReliableTaskQueue`, `State`, `FailureOutcome`, `Config`, `Request`, `Lease`, `View`, `Event`, `Stats` |
| [src/main/java/dev/juniordev/TaskQueue.java](../src/main/java/dev/juniordev/TaskQueue.java) | Implementação | `TaskQueue`, `Job`, `Result`, `Stats`, `submit`, `processNext`, `pending`, `stats`, `deadLetters` |
| [src/main/java/dev/juniordev/TaskQueueReports.java](../src/main/java/dev/juniordev/TaskQueueReports.java) | Implementação | `TaskQueueReports`, `gauge`, `counter`, `number`, `nullableNumber`, `field`, `jobsCsv`, `auditCsv`, `jobsJson` |
| [src/main/java/dev/juniordev/TaskQueueWorker.java](../src/main/java/dev/juniordev/TaskQueueWorker.java) | Implementação | `TaskQueueWorker`, `Handler`, `handle`, `Outcome`, `Attempt`, `Batch`, `succeeded`, `failed`, `successes` |

## Configuração e referências

Ambiente e dependências: [pom.xml](../pom.xml).

Referências existentes: [docs/GUIA_DE_VALIDACAO.md](GUIA_DE_VALIDACAO.md).

## Roteiro de estudo e desenvolvimento

1. Comece pelo fluxo descrito acima e localize seus pontos de entrada no mapa. Acompanhe uma entrada válida até a transformação ou a resposta.
2. Abra um teste relacionado ao fluxo e descreva qual regra ele protege. Compare o tratamento esperado para entradas válidas, inválidas e nos limites.
3. Ao adicionar uma regra, determine qual módulo controla o dado e cubra o caso nesse nível. Em seguida, confira o efeito no comando, na API ou na interface de entrada.

**Pergunta de entrevista**

Como evitar que um resultado atrasado conclua uma tarefa já atribuída a outro processamento?

## Testes versionados

Arquivos para começar a leitura dos cenários de teste: [src/test/java/dev/juniordev/ReliableTaskQueueTest.java](../src/test/java/dev/juniordev/ReliableTaskQueueTest.java), [src/test/java/dev/juniordev/TaskQueueTest.java](../src/test/java/dev/juniordev/TaskQueueTest.java), [src/test/java/dev/juniordev/TaskQueueWorkerReportsTest.java](../src/test/java/dev/juniordev/TaskQueueWorkerReportsTest.java).

Os comandos e versões de runtime usados no CI ficam em [.github/workflows/ci.yml](../.github/workflows/ci.yml).

[Voltar ao README](../README.md).
