# Guia de validação — Distributed Task Queue

## Estado e abrangência

Fila Java 17 em memória e em processo único; o nome não implica distribuição entre servidores.

## Fontes para revisão

`pom.xml`, `src/main/java/dev/juniordev/TaskQueue.java` e `src/test/java/dev/juniordev/TaskQueueTest.java`.

## Comandos de verificação

`mvn -B test` com JDK 17.

Os testes automatizados devem ser revisados junto do código. Caso não tenham sido executados, registrar essa limitação no PR.

## Matriz de regressão

Verificar retentativas, exceções de worker, estado DEAD_LETTER, concorrência no enqueue e métricas que não contam tarefa já em execução como pendente.

## Próximo incremento fundamentado

Antes de distribuir a fila, projetar durabilidade, idempotência de consumidores e visibilidade/lease de tarefas.

## Critérios de contribuição

- [ ] Alteração atende comportamento verificável.
- [ ] Foi criada evidência de teste ou procedimento reproduzível.
- [ ] Nenhuma credencial ou dado pessoal foi versionado.
- [ ] O PR identifica claramente o que mudou e o que ainda falta.
