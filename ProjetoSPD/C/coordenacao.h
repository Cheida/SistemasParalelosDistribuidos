#ifndef COORDENACAO_H
#define COORDENACAO_H
#include "referencia.h"

/* Parte 4: eleição (Bully) e sincronização de Berkeley entre os servidores. */
int coordenacao_iniciar(void *contexto, Referencia *referencia);
void coordenacao_contar_mensagem(void);
void coordenacao_encerrar(void);
#endif
