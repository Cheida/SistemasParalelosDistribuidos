#ifndef REFERENCIA_H
#define REFERENCIA_H
#include "protocolo.h"

/* Estado do cadastro deste servidor; o relógio fica em protocolo.c. */
typedef struct {
    void *contexto;
    const char *nome;
    const char *endereco;
    int64_t rank;
    double proxima_tentativa;
} Referencia;

bool referencia_registrar(Referencia *referencia);
void referencia_listar(Referencia *referencia);
void referencia_manter(Referencia *referencia);
void referencia_heartbeat(Referencia *referencia);
#endif
