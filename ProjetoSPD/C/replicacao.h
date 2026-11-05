#ifndef REPLICACAO_H
#define REPLICACAO_H

/* Parte 5: replicação assíncrona das quatro tabelas entre os servidores,
 * no mesmo protocolo do python/replicacao.py (REP na porta 5561). */
int replicacao_iniciar(void *contexto, const char *nome, const char *arquivo_banco);
void replicacao_encerrar(void);

#endif
