#ifndef PROTOCOLO_H
#define PROTOCOLO_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include <msgpack.h>

int64_t timestamp_ms(void);

double agora_monotonico(void);

const char *ambiente(const char *nome, const char *padrao);

bool mensagens_completas(void);

int decodificar(const char *dados, size_t tamanho, msgpack_unpacked *resultado);

bool utf8_valido(const char *texto, size_t tamanho);


const msgpack_object *buscar(const msgpack_object *mapa, const char *chave);

bool texto_igual(const msgpack_object *objeto, const char *texto);

bool eh_inteiro(const msgpack_object *objeto);

void imprimir_objeto(FILE *saida, const msgpack_object *objeto, bool repr);

int empacotar_texto(msgpack_packer *empacotador, const char *texto, size_t tamanho);

#endif
