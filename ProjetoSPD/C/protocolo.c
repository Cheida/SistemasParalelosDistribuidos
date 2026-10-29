#define _POSIX_C_SOURCE 200809L

#include "protocolo.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int64_t contador = 0;
static int64_t ajuste_ms = 0;

int64_t relogio_atual(void) { return contador; }

int64_t relogio_enviar(void)
{
    if (contador == INT64_MAX) {
        fputs("Contador lógico esgotado.\n", stderr);
        exit(EXIT_FAILURE);
    }
    return ++contador;
}

int relogio_receber(const msgpack_object *mensagem)
{
    const msgpack_object *clock = buscar(mensagem, "clock");
    if (clock == NULL || clock->type != MSGPACK_OBJECT_POSITIVE_INTEGER ||
        clock->via.u64 > INT64_MAX) return -1;
    /* Não somamos 1 ao receber: essa é a regra do enunciado. */
    if ((int64_t)clock->via.u64 > contador) contador = (int64_t)clock->via.u64;
    return 0;
}

void relogio_iniciar_servidor(void)
{
    ajuste_ms = strtoll(ambiente("DESVIO_INICIAL_MS", "0"), NULL, 10);
}

int64_t relogio_sincronizar(int64_t referencia, double duracao)
{
    int64_t hora_sistema = timestamp_ms() - ajuste_ms;
    ajuste_ms = referencia + (int64_t)(duracao * 500.0 + 0.5) - hora_sistema;
    return ajuste_ms;
}

int empacotar_clock(msgpack_packer *pk)
{
    int erro = empacotar_texto(pk, "clock", 5);
    erro |= msgpack_pack_int64(pk, relogio_enviar());
    return erro;
}

int64_t timestamp_ms(void)
{
    struct timespec agora;
    clock_gettime(CLOCK_REALTIME, &agora);
    return (int64_t)agora.tv_sec * 1000 + agora.tv_nsec / 1000000 + ajuste_ms;
}

double agora_monotonico(void)
{
    struct timespec agora;
    clock_gettime(CLOCK_MONOTONIC, &agora);
    return (double)agora.tv_sec + (double)agora.tv_nsec / 1e9;
}

const char *ambiente(const char *nome, const char *padrao)
{
    const char *valor = getenv(nome);
    return valor != NULL ? valor : padrao;
}

bool mensagens_completas(void)
{
    return strcmp(ambiente("MENSAGENS_COMPLETAS", "0"), "1") == 0;
}

bool utf8_valido(const char *texto, size_t tamanho)
{
    const unsigned char *p = (const unsigned char *)texto;
    size_t i = 0;
    while (i < tamanho) {
        unsigned char c = p[i];
        size_t extras;
        uint32_t ponto;
        if (c < 0x80) {
            i++;
            continue;
        } else if (c >= 0xC2 && c <= 0xDF) {
            extras = 1;
            ponto = c & 0x1F;
        } else if (c >= 0xE0 && c <= 0xEF) {
            extras = 2;
            ponto = c & 0x0F;
        } else if (c >= 0xF0 && c <= 0xF4) {
            extras = 3;
            ponto = c & 0x07;
        } else {
            return false;
        }
        if (tamanho - i <= extras) {
            return false;
        }
        for (size_t k = 1; k <= extras; k++) {
            if ((p[i + k] & 0xC0) != 0x80) {
                return false;
            }
            ponto = (ponto << 6) | (p[i + k] & 0x3F);
        }
        
        if ((extras == 2 && ponto < 0x800) || (extras == 3 && ponto < 0x10000) ||
            (ponto >= 0xD800 && ponto <= 0xDFFF) || ponto > 0x10FFFF) {
            return false;
        }
        i += extras + 1;
    }
    return true;
}


static bool objeto_valido(const msgpack_object *objeto)
{
    switch (objeto->type) {
    case MSGPACK_OBJECT_STR:
        return utf8_valido(objeto->via.str.ptr, objeto->via.str.size);
    case MSGPACK_OBJECT_ARRAY:
        for (uint32_t i = 0; i < objeto->via.array.size; i++) {
            if (!objeto_valido(&objeto->via.array.ptr[i])) {
                return false;
            }
        }
        return true;
    case MSGPACK_OBJECT_MAP:
        for (uint32_t i = 0; i < objeto->via.map.size; i++) {
            const msgpack_object_kv *par = &objeto->via.map.ptr[i];
            if (par->key.type != MSGPACK_OBJECT_STR && par->key.type != MSGPACK_OBJECT_BIN) {
                return false;
            }
            if (!objeto_valido(&par->key) || !objeto_valido(&par->val)) {
                return false;
            }
        }
        return true;
    default:
        return true;
    }
}

int decodificar(const char *dados, size_t tamanho, msgpack_unpacked *resultado)
{
    size_t posicao = 0;
    if (dados == NULL || tamanho == 0) {
        return -1;
    }
    if (msgpack_unpack_next(resultado, dados, tamanho, &posicao) != MSGPACK_UNPACK_SUCCESS) {
        return -1;
    }
    if (posicao != tamanho || !objeto_valido(&resultado->data)) {
        return -1;
    }
    return 0;
}

const msgpack_object *buscar(const msgpack_object *mapa, const char *chave)
{
    const msgpack_object *encontrado = NULL;
    size_t tamanho = strlen(chave);
    if (mapa == NULL || mapa->type != MSGPACK_OBJECT_MAP) {
        return NULL;
    }
    for (uint32_t i = 0; i < mapa->via.map.size; i++) {
        const msgpack_object *k = &mapa->via.map.ptr[i].key;
        if (k->type == MSGPACK_OBJECT_STR && k->via.str.size == tamanho &&
            memcmp(k->via.str.ptr, chave, tamanho) == 0) {
            encontrado = &mapa->via.map.ptr[i].val;
        }
    }
    return encontrado;
}

bool texto_igual(const msgpack_object *objeto, const char *texto)
{
    size_t tamanho = strlen(texto);
    return objeto != NULL && objeto->type == MSGPACK_OBJECT_STR &&
           objeto->via.str.size == tamanho && memcmp(objeto->via.str.ptr, texto, tamanho) == 0;
}

bool eh_inteiro(const msgpack_object *objeto)
{
    return objeto != NULL && (objeto->type == MSGPACK_OBJECT_POSITIVE_INTEGER ||
                              objeto->type == MSGPACK_OBJECT_NEGATIVE_INTEGER);
}

static void imprimir_citado(FILE *saida, const char *dados, size_t tamanho, bool binario)
{
    char aspas = '\'';
    if (memchr(dados, '\'', tamanho) != NULL && memchr(dados, '"', tamanho) == NULL) {
        aspas = '"';
    }
    if (binario) {
        fputc('b', saida);
    }
    fputc(aspas, saida);
    for (size_t i = 0; i < tamanho; i++) {
        unsigned char c = (unsigned char)dados[i];
        if (c == '\\' || c == (unsigned char)aspas) {
            fprintf(saida, "\\%c", c);
        } else if (c == '\n') {
            fputs("\\n", saida);
        } else if (c == '\r') {
            fputs("\\r", saida);
        } else if (c == '\t') {
            fputs("\\t", saida);
        } else if (c < 0x20 || c == 0x7F || (binario && c > 0x7F)) {
            fprintf(saida, "\\x%02x", c);
        } else {
            fputc(c, saida);
        }
    }
    fputc(aspas, saida);
}

static void imprimir_real(FILE *saida, double valor)
{
    char texto[64];
    if (isnan(valor)) {
        fputs("nan", saida);
        return;
    }
    if (isinf(valor)) {
        fputs(valor < 0 ? "-inf" : "inf", saida);
        return;
    }
    for (int precisao = 1; precisao <= 17; precisao++) {
        snprintf(texto, sizeof texto, "%.*g", precisao, valor);
        if (strtod(texto, NULL) == valor) {
            break;
        }
    }
    fputs(texto, saida);
    if (strpbrk(texto, ".e") == NULL) {
        fputs(".0", saida);
    }
}

void imprimir_objeto(FILE *saida, const msgpack_object *objeto, bool repr)
{
    switch (objeto->type) {
    case MSGPACK_OBJECT_NIL:
        fputs("None", saida);
        break;
    case MSGPACK_OBJECT_BOOLEAN:
        fputs(objeto->via.boolean ? "True" : "False", saida);
        break;
    case MSGPACK_OBJECT_POSITIVE_INTEGER:
        fprintf(saida, "%llu", (unsigned long long)objeto->via.u64);
        break;
    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
        fprintf(saida, "%lld", (long long)objeto->via.i64);
        break;
    case MSGPACK_OBJECT_FLOAT32:
    case MSGPACK_OBJECT_FLOAT64:
        imprimir_real(saida, objeto->via.f64);
        break;
    case MSGPACK_OBJECT_STR:
        if (repr) {
            imprimir_citado(saida, objeto->via.str.ptr, objeto->via.str.size, false);
        } else {
            fwrite(objeto->via.str.ptr, 1, objeto->via.str.size, saida);
        }
        break;
    case MSGPACK_OBJECT_BIN:
        imprimir_citado(saida, objeto->via.bin.ptr, objeto->via.bin.size, true);
        break;
    case MSGPACK_OBJECT_ARRAY:
        fputc('[', saida);
        for (uint32_t i = 0; i < objeto->via.array.size; i++) {
            if (i > 0) {
                fputs(", ", saida);
            }
            imprimir_objeto(saida, &objeto->via.array.ptr[i], true);
        }
        fputc(']', saida);
        break;
    case MSGPACK_OBJECT_MAP:
        fputc('{', saida);
        for (uint32_t i = 0; i < objeto->via.map.size; i++) {
            if (i > 0) {
                fputs(", ", saida);
            }
            imprimir_objeto(saida, &objeto->via.map.ptr[i].key, true);
            fputs(": ", saida);
            imprimir_objeto(saida, &objeto->via.map.ptr[i].val, true);
        }
        fputc('}', saida);
        break;
    case MSGPACK_OBJECT_EXT:
        fprintf(saida, "ExtType(code=%d, data=", (int)objeto->via.ext.type);
        imprimir_citado(saida, objeto->via.ext.ptr, objeto->via.ext.size, true);
        fputc(')', saida);
        break;
    default:
        fputs("?", saida);
        break;
    }
}

int empacotar_texto(msgpack_packer *empacotador, const char *texto, size_t tamanho)
{
    if (msgpack_pack_str(empacotador, tamanho) != 0) {
        return -1;
    }
    return msgpack_pack_str_body(empacotador, texto, tamanho);
}
