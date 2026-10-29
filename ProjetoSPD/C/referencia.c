#define _POSIX_C_SOURCE 200809L
#include "referencia.h"
#include <string.h>
#include <zmq.h>

/* Fazemos um pedido de cada vez, com prazo, para a referência poder reiniciar. */
static int pedir(Referencia *ref, const char *operacao)
{
    int resultado = -1;
    void *socket = zmq_socket(ref->contexto, ZMQ_REQ);
    if (socket == NULL) return -1;
    int zero = 0, prazo = 1000;
    zmq_setsockopt(socket, ZMQ_LINGER, &zero, sizeof zero);
    zmq_setsockopt(socket, ZMQ_SNDTIMEO, &prazo, sizeof prazo);
    zmq_setsockopt(socket, ZMQ_RCVTIMEO, &prazo, sizeof prazo);
    msgpack_sbuffer pedido;
    msgpack_sbuffer_init(&pedido);
    msgpack_unpacked resposta;
    msgpack_unpacked_init(&resposta);
    zmq_msg_t mensagem;
    zmq_msg_init(&mensagem);
    if (zmq_connect(socket, ref->endereco) != 0) goto fim;

    msgpack_packer pk;
    msgpack_packer_init(&pk, &pedido, msgpack_sbuffer_write);
    bool cadastro = strcmp(operacao, "register_server") == 0;
    bool lista = strcmp(operacao, "list_servers") == 0;
    int erro = msgpack_pack_map(&pk, 4);
    erro |= empacotar_texto(&pk, "operation", 9);
    erro |= empacotar_texto(&pk, operacao, strlen(operacao));
    erro |= empacotar_texto(&pk, "timestamp", 9);
    erro |= msgpack_pack_int64(&pk, timestamp_ms());
    erro |= empacotar_clock(&pk);
    erro |= empacotar_texto(&pk, "payload", 7);
    erro |= msgpack_pack_map(&pk, lista ? 0 : (cadastro ? 2 : 1));
    if (!lista) {
        erro |= empacotar_texto(&pk, "name", 4);
        erro |= empacotar_texto(&pk, ref->nome, strlen(ref->nome));
    }
    if (cadastro) {
        const char *host = ambiente("ENDERECO_SERVIDOR", ambiente("HOSTNAME", "localhost"));
        erro |= empacotar_texto(&pk, "address", 7);
        erro |= empacotar_texto(&pk, host, strlen(host));
    }
    if (erro) goto fim;
    double inicio = agora_monotonico();
    if (zmq_send(socket, pedido.data, pedido.size, 0) < 0 ||
        zmq_msg_recv(&mensagem, socket, 0) < 0) goto fim;
    double duracao = agora_monotonico() - inicio;
    if (decodificar(zmq_msg_data(&mensagem), zmq_msg_size(&mensagem), &resposta) != 0 ||
        relogio_receber(&resposta.data) != 0 ||
        !texto_igual(buscar(&resposta.data, "operation"), operacao)) goto fim;
    const msgpack_object *status = buscar(&resposta.data, "status");
    if (texto_igual(status, "server_not_registered")) {
        ref->rank = 0;
        resultado = 1;
        goto fim;
    }
    if (!texto_igual(status, "ok")) goto fim;
    const msgpack_object *dados = buscar(&resposta.data, "payload");
    if (dados == NULL || dados->type != MSGPACK_OBJECT_MAP) goto fim;
    if (lista) {
        printf("[%s] SERVIDORES | ", ref->nome);
        imprimir_objeto(stdout, dados, true);
        putchar('\n');
    } else {
        const msgpack_object *rank = buscar(dados, "rank");
        if (rank == NULL || rank->type != MSGPACK_OBJECT_POSITIVE_INTEGER ||
            rank->via.u64 == 0 || rank->via.u64 > INT64_MAX) goto fim;
        ref->rank = (int64_t)rank->via.u64;
        if (cadastro) {
            printf("[%s] REGISTRADO | rank: %lld | clock: %lld\n", ref->nome,
                   (long long)ref->rank, (long long)relogio_atual());
        } else {
            const msgpack_object *hora = buscar(&resposta.data, "timestamp");
            if (hora == NULL || hora->type != MSGPACK_OBJECT_POSITIVE_INTEGER ||
                hora->via.u64 > INT64_MAX) goto fim;
            int64_t ajuste = relogio_sincronizar((int64_t)hora->via.u64, duracao);
            printf("[%s] HEARTBEAT | rank: %lld | ajuste físico: %lldms | clock: %lld\n",
                   ref->nome, (long long)ref->rank, (long long)ajuste, (long long)relogio_atual());
        }
    }
    resultado = 0;
fim:
    if (resultado < 0) printf("[%s] Referência indisponível; tentaremos novamente.\n", ref->nome);
    fflush(stdout);
    zmq_msg_close(&mensagem);
    msgpack_unpacked_destroy(&resposta);
    msgpack_sbuffer_destroy(&pedido);
    zmq_close(socket);
    return resultado;
}

bool referencia_registrar(Referencia *ref)
{
    ref->proxima_tentativa = agora_monotonico() + 2;
    return pedir(ref, "register_server") == 0;
}

void referencia_listar(Referencia *ref) { (void)pedir(ref, "list_servers"); }

void referencia_manter(Referencia *ref)
{
    if (ref->rank == 0 && agora_monotonico() >= ref->proxima_tentativa)
        (void)referencia_registrar(ref);
}

void referencia_heartbeat(Referencia *ref)
{
    if (ref->rank == 0 && !referencia_registrar(ref)) return;
    if (pedir(ref, "heartbeat") == 1) (void)referencia_registrar(ref);
}
