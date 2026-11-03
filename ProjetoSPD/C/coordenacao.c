/* Parte 4: eleição do coordenador (Bully) e sincronização de Berkeley.
 * Uma thread atende o REP de controle e o tópico "servers"; outra faz
 * descoberta, eleição, verificação do coordenador e as rodadas de Berkeley. */
#define _POSIX_C_SOURCE 200809L
#include "coordenacao.h"

#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uuid/uuid.h>
#include <zmq.h>

#define MAX_SERVIDORES 64
#define MAX_RODADAS 64
#define TOPICO "\xa7servers"
#define TAMANHO_TOPICO 8

typedef struct {
    char nome[65];
    int64_t rank;
    char endereco[256];
} Par;

typedef struct {
    msgpack_unpacked dados;
    zmq_msg_t bruto;
    const msgpack_object *payload;
    const msgpack_object *status;
    int64_t timestamp;
    double duracao;
    bool ok;
} Rpc;

typedef struct {
    const char *status;
    char rodada[37];
    bool aplicado;
} Rodada;

typedef struct {
    msgpack_sbuffer buf;
    msgpack_packer pk;
} Carga;

static void *contexto;
static Referencia *ref;
static pthread_t ouvinte, trabalhador;
static bool iniciado;
static atomic_bool parar;
static pthread_mutex_t trava = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sinal = PTHREAD_COND_INITIALIZER;
static int pronto;

static bool tem_lider, eleicao;
static Par lider;
static Par servidores[MAX_SERVIDORES];
static int quantidade;
static int mensagens, pendentes;
static double aguardar;
static int64_t rodadas;
static char ultima_rodada[37];
static char aplicadas[MAX_RODADAS][37];
static int proxima_aplicada;

static bool sync_pedido, sync_pronto;
static char sync_nome[65];
static Rodada sync_resultado;

static void dormir(double segundos)
{
    struct timespec t = {(time_t)segundos, (long)((segundos - (time_t)segundos) * 1e9)};
    nanosleep(&t, NULL);
}

static void carga_iniciar(Carga *c)
{
    msgpack_sbuffer_init(&c->buf);
    msgpack_packer_init(&c->pk, &c->buf, msgpack_sbuffer_write);
}

static void texto(msgpack_packer *pk, const char *s) { empacotar_texto(pk, s, strlen(s)); }

static void campo_texto(msgpack_packer *pk, const char *chave, const char *valor)
{
    texto(pk, chave);
    if (valor != NULL) texto(pk, valor);
    else msgpack_pack_nil(pk);
}

static void campo_int(msgpack_packer *pk, const char *chave, int64_t valor)
{
    texto(pk, chave);
    msgpack_pack_int64(pk, valor);
}

static bool ler_texto(const msgpack_object *o, char *destino, size_t tamanho)
{
    if (o == NULL || o->type != MSGPACK_OBJECT_STR || o->via.str.size == 0 ||
        o->via.str.size >= tamanho) return false;
    memcpy(destino, o->via.str.ptr, o->via.str.size);
    destino[o->via.str.size] = '\0';
    return true;
}

static bool ler_int(const msgpack_object *o, int64_t *valor)
{
    if (o == NULL) return false;
    if (o->type == MSGPACK_OBJECT_POSITIVE_INTEGER && o->via.u64 <= INT64_MAX) {
        *valor = (int64_t)o->via.u64;
        return true;
    }
    if (o->type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
        *valor = o->via.i64;
        return true;
    }
    return false;
}

static bool ler_par(const msgpack_object *o, Par *p)
{
    return ler_texto(buscar(o, "name"), p->nome, sizeof p->nome) &&
           ler_int(buscar(o, "rank"), &p->rank) && p->rank > 0 &&
           ler_texto(buscar(o, "address"), p->endereco, sizeof p->endereco) &&
           strncmp(p->endereco, "tcp://", 6) == 0;
}

static Par eu_par(void)
{
    Par p;
    snprintf(p.nome, sizeof p.nome, "%s", ref->nome);
    p.rank = ref->rank;
    snprintf(p.endereco, sizeof p.endereco, "%s", ref->endereco_servidor);
    return p;
}

static void eu(msgpack_packer *pk)
{
    msgpack_pack_map(pk, 3);
    campo_texto(pk, "name", ref->nome);
    campo_int(pk, "rank", ref->rank);
    campo_texto(pk, "address", ref->endereco_servidor);
}

static void status(msgpack_packer *pk)
{
    pthread_mutex_lock(&trava);
    msgpack_pack_map(pk, 8);
    campo_texto(pk, "name", ref->nome);
    campo_int(pk, "rank", ref->rank);
    campo_texto(pk, "coordinator", tem_lider ? lider.nome : NULL);
    campo_int(pk, "time_ms", timestamp_ms());
    campo_int(pk, "adjustment_ms", relogio_ajuste_atual());
    campo_int(pk, "rounds", rodadas);
    campo_texto(pk, "last_round", ultima_rodada[0] ? ultima_rodada : NULL);
    campo_int(pk, "messages_since_sync", mensagens);
    pthread_mutex_unlock(&trava);
}

static void envelope(msgpack_sbuffer *saida, const char *operacao, const char *st,
                     const msgpack_sbuffer *payload)
{
    msgpack_packer pk;
    msgpack_packer_init(&pk, saida, msgpack_sbuffer_write);
    msgpack_pack_map(&pk, st != NULL ? 5 : 4);
    campo_texto(&pk, "operation", operacao);
    campo_int(&pk, "timestamp", timestamp_ms());
    empacotar_clock(&pk);
    if (st != NULL) campo_texto(&pk, "status", st);
    texto(&pk, "payload");
    if (payload != NULL && payload->size > 0) msgpack_sbuffer_write(saida, payload->data, payload->size);
    else msgpack_pack_map(&pk, 0);
}

static bool sou_lider(void)
{
    pthread_mutex_lock(&trava);
    bool sou = tem_lider && strcmp(lider.nome, ref->nome) == 0;
    pthread_mutex_unlock(&trava);
    return sou;
}

static int copiar_servidores(Par *lista)
{
    pthread_mutex_lock(&trava);
    int n = quantidade;
    memcpy(lista, servidores, sizeof(Par) * (size_t)n);
    pthread_mutex_unlock(&trava);
    return n;
}

/* Pedido REQ/REP com prazo; quem chama sempre libera com rpc_liberar. */
static bool rpc(const char *endereco, const char *operacao, const msgpack_sbuffer *payload,
                int prazo, Rpc *r)
{
    bool valido = false;
    r->ok = false;
    msgpack_unpacked_init(&r->dados);
    zmq_msg_init(&r->bruto);
    msgpack_sbuffer pedido;
    msgpack_sbuffer_init(&pedido);
    void *s = zmq_socket(contexto, ZMQ_REQ);
    if (s == NULL) goto fim;
    int zero = 0;
    zmq_setsockopt(s, ZMQ_LINGER, &zero, sizeof zero);
    zmq_setsockopt(s, ZMQ_SNDTIMEO, &prazo, sizeof prazo);
    zmq_setsockopt(s, ZMQ_RCVTIMEO, &prazo, sizeof prazo);
    if (zmq_connect(s, endereco) != 0) goto fim;
    envelope(&pedido, operacao, NULL, payload);
    double inicio = agora_monotonico();
    if (zmq_send(s, pedido.data, pedido.size, 0) < 0 || zmq_msg_recv(&r->bruto, s, 0) < 0) goto fim;
    r->duracao = agora_monotonico() - inicio;
    if (decodificar(zmq_msg_data(&r->bruto), zmq_msg_size(&r->bruto), &r->dados) != 0 ||
        relogio_receber(&r->dados.data) != 0) goto fim;
    const msgpack_object *ts = buscar(&r->dados.data, "timestamp");
    r->status = buscar(&r->dados.data, "status");
    r->payload = buscar(&r->dados.data, "payload");
    if (!texto_igual(buscar(&r->dados.data, "operation"), operacao) || ts == NULL ||
        ts->type != MSGPACK_OBJECT_POSITIVE_INTEGER || ts->via.u64 > INT64_MAX ||
        r->status == NULL || r->status->type != MSGPACK_OBJECT_STR ||
        r->payload == NULL || r->payload->type != MSGPACK_OBJECT_MAP) goto fim;
    r->timestamp = (int64_t)ts->via.u64;
    r->ok = texto_igual(r->status, "ok");
    valido = true;
fim:
    msgpack_sbuffer_destroy(&pedido);
    if (s != NULL) zmq_close(s);
    return valido;
}

static void rpc_liberar(Rpc *r)
{
    msgpack_unpacked_destroy(&r->dados);
    zmq_msg_close(&r->bruto);
}

static bool rpc_ok(const char *endereco, const char *operacao, const msgpack_sbuffer *payload, int prazo)
{
    Rpc r;
    bool ok = rpc(endereco, operacao, payload, prazo, &r) && r.ok;
    rpc_liberar(&r);
    return ok;
}

/* Aceita o candidato se o rank dele for o maior conhecido. */
static bool aceitar(const Par *c)
{
    if (ref->rank == 0) return false;
    bool aceito = false, mudou = false;
    pthread_mutex_lock(&trava);
    if (c->rank < ref->rank) {
        eleicao = true;
    } else if (!(tem_lider && c->rank < lider.rank)) {
        mudou = !tem_lider || strcmp(lider.nome, c->nome) != 0;
        lider = *c;
        tem_lider = true;
        aguardar = 0;
        aceito = true;
    }
    pthread_mutex_unlock(&trava);
    if (mudou) {
        printf("[%s] COORDENADOR DEFINIDO | nome: %s | rank: %lld\n", ref->nome, c->nome,
               (long long)c->rank);
        fflush(stdout);
    }
    return aceito;
}

static void perder(const Par *antigo)
{
    pthread_mutex_lock(&trava);
    if (tem_lider && strcmp(lider.nome, antigo->nome) == 0) {
        tem_lider = false;
        eleicao = true;
        printf("[%s] COORDENADOR INDISPONÍVEL | %s\n", ref->nome, antigo->nome);
        fflush(stdout);
    }
    pthread_mutex_unlock(&trava);
}

static const char *ajustar(const msgpack_object *dados, Carga *saida)
{
    char coordenador[65], rodada[37];
    int64_t rank, ajuste;
    const char *st = "ok";
    pthread_mutex_lock(&trava);
    if (!tem_lider || !ler_texto(buscar(dados, "coordinator"), coordenador, sizeof coordenador) ||
        strcmp(coordenador, lider.nome) != 0 || !ler_int(buscar(dados, "rank"), &rank) ||
        rank != lider.rank) {
        st = "not_coordinator";
    } else if (!ler_texto(buscar(dados, "round_id"), rodada, sizeof rodada) ||
               !ler_int(buscar(dados, "adjustment_ms"), &ajuste)) {
        st = "invalid_request";
    } else {
        bool repetida = false;
        for (int i = 0; i < MAX_RODADAS; i++)
            if (strcmp(aplicadas[i], rodada) == 0) repetida = true;
        if (!repetida) {
            relogio_aplicar_ajuste(ajuste);
            strcpy(aplicadas[proxima_aplicada], rodada);
            proxima_aplicada = (proxima_aplicada + 1) % MAX_RODADAS;
            strcpy(ultima_rodada, rodada);
            printf("[%s] BERKELEY | correção: %lldms | coordenador: %s\n", ref->nome,
                   (long long)ajuste, lider.nome);
            fflush(stdout);
        }
        msgpack_pack_map(&saida->pk, 1);
        campo_texto(&saida->pk, "round_id", rodada);
    }
    pthread_mutex_unlock(&trava);
    return st;
}

/* clock_sync recebido: a thread de trabalho roda Berkeley e nos devolve o resultado. */
static const char *pedir_rodada(const msgpack_object *dados, Carga *saida)
{
    if (!sou_lider()) {
        status(&saida->pk);
        return "not_coordinator";
    }
    char nome[65];
    if (!ler_texto(buscar(dados, "name"), nome, sizeof nome)) return "invalid_request";
    struct timespec limite;
    clock_gettime(CLOCK_REALTIME, &limite);
    limite.tv_sec += 15;
    pthread_mutex_lock(&trava);
    strcpy(sync_nome, nome);
    sync_pedido = true;
    sync_pronto = false;
    while (!sync_pronto && !parar)
        if (pthread_cond_timedwait(&sinal, &trava, &limite) == ETIMEDOUT) break;
    bool concluido = sync_pronto;
    Rodada r = sync_resultado;
    sync_pedido = false;
    sync_pronto = false;
    pthread_mutex_unlock(&trava);
    if (!concluido) return "sync_timeout";
    msgpack_pack_map(&saida->pk, 4);
    campo_texto(&saida->pk, "coordinator", ref->nome);
    campo_int(&saida->pk, "time_ms", timestamp_ms());
    campo_texto(&saida->pk, "round_id", r.rodada[0] ? r.rodada : NULL);
    texto(&saida->pk, "applied");
    if (r.aplicado) msgpack_pack_true(&saida->pk);
    else msgpack_pack_false(&saida->pk);
    return r.status;
}

static const char *atender(const msgpack_object *pedido, const msgpack_object *operacao, Carga *saida)
{
    const msgpack_object *dados = buscar(pedido, "payload");
    const msgpack_object *ts = buscar(pedido, "timestamp");
    if (dados == NULL || dados->type != MSGPACK_OBJECT_MAP || ts == NULL ||
        ts->type != MSGPACK_OBJECT_POSITIVE_INTEGER) return "invalid_request";

    if (texto_igual(operacao, "election")) {
        int64_t rank;
        if (!ler_int(buscar(dados, "rank"), &rank)) return "invalid_request";
        if (ref->rank > rank) {
            pthread_mutex_lock(&trava);
            eleicao = true;
            pthread_mutex_unlock(&trava);
        }
        eu(&saida->pk);
        return "ok";
    }
    if (texto_igual(operacao, "coordinator")) {
        Par c;
        return ler_par(dados, &c) && aceitar(&c) ? "ok" : "lower_rank";
    }
    if (texto_igual(operacao, "server_status") || texto_igual(operacao, "clock_read")) {
        status(&saida->pk);
        return "ok";
    }
    if (texto_igual(operacao, "coordinator_status")) {
        status(&saida->pk);
        return sou_lider() ? "ok" : "not_coordinator";
    }
    if (texto_igual(operacao, "clock_adjust")) return ajustar(dados, saida);
    if (texto_igual(operacao, "clock_sync")) return pedir_rodada(dados, saida);
    return "unknown_operation";
}

static void responder(void *rep)
{
    zmq_msg_t m;
    zmq_msg_init(&m);
    if (zmq_msg_recv(&m, rep, 0) < 0) {
        zmq_msg_close(&m);
        return;
    }
    msgpack_unpacked pedido;
    msgpack_unpacked_init(&pedido);
    Carga carga;
    carga_iniciar(&carga);
    char operacao[64] = "";
    const char *st = "invalid_request";
    if (decodificar(zmq_msg_data(&m), zmq_msg_size(&m), &pedido) == 0 &&
        relogio_receber(&pedido.data) == 0) {
        const msgpack_object *op = buscar(&pedido.data, "operation");
        if (ler_texto(op, operacao, sizeof operacao)) st = atender(&pedido.data, op, &carga);
    }
    msgpack_sbuffer saida;
    msgpack_sbuffer_init(&saida);
    envelope(&saida, operacao, st, &carga.buf);
    zmq_send(rep, saida.data, saida.size, 0);
    msgpack_sbuffer_destroy(&saida);
    msgpack_sbuffer_destroy(&carga.buf);
    msgpack_unpacked_destroy(&pedido);
    zmq_msg_close(&m);
}

static void receber_anuncio(void *sub)
{
    zmq_msg_t topico, corpo;
    zmq_msg_init(&topico);
    zmq_msg_init(&corpo);
    int frames = 0;
    if (zmq_msg_recv(&topico, sub, 0) >= 0) frames = 1;
    while (frames > 0 && zmq_msg_more(frames == 1 ? &topico : &corpo)) {
        zmq_msg_close(&corpo);
        zmq_msg_init(&corpo);
        if (zmq_msg_recv(&corpo, sub, 0) < 0) break;
        frames++;
    }
    msgpack_unpacked evento;
    msgpack_unpacked_init(&evento);
    Par c;
    if (frames == 2 && zmq_msg_size(&topico) == TAMANHO_TOPICO &&
        memcmp(zmq_msg_data(&topico), TOPICO, TAMANHO_TOPICO) == 0 &&
        decodificar(zmq_msg_data(&corpo), zmq_msg_size(&corpo), &evento) == 0 &&
        relogio_receber(&evento.data) == 0 &&
        texto_igual(buscar(&evento.data, "operation"), "coordinator") &&
        ler_par(buscar(&evento.data, "payload"), &c))
        aceitar(&c);
    msgpack_unpacked_destroy(&evento);
    zmq_msg_close(&topico);
    zmq_msg_close(&corpo);
}

static void *ouvir(void *arg)
{
    (void)arg;
    int zero = 0;
    void *rep = zmq_socket(contexto, ZMQ_REP);
    void *sub = zmq_socket(contexto, ZMQ_SUB);
    bool ok = rep != NULL && sub != NULL;
    if (ok) {
        zmq_setsockopt(rep, ZMQ_LINGER, &zero, sizeof zero);
        zmq_setsockopt(sub, ZMQ_LINGER, &zero, sizeof zero);
        ok = zmq_bind(rep, ambiente("ENDERECO_CONTROLE_BIND", "tcp://*:5560")) == 0 &&
             zmq_connect(sub, ambiente("ENDERECO_INSCRICAO", "tcp://localhost:5558")) == 0 &&
             zmq_setsockopt(sub, ZMQ_SUBSCRIBE, TOPICO, TAMANHO_TOPICO) == 0;
    }
    if (!ok) printf("[%s] Erro no controle: %s\n", ref->nome, zmq_strerror(zmq_errno()));
    pthread_mutex_lock(&trava);
    pronto = ok ? 1 : -1;
    pthread_cond_broadcast(&sinal);
    pthread_mutex_unlock(&trava);

    zmq_pollitem_t itens[2] = {{.socket = rep, .events = ZMQ_POLLIN},
                               {.socket = sub, .events = ZMQ_POLLIN}};
    while (ok && !parar) {
        if (zmq_poll(itens, 2, 30) <= 0) continue;
        if (itens[1].revents & ZMQ_POLLIN) receber_anuncio(sub);
        if (itens[0].revents & ZMQ_POLLIN) responder(rep);
    }
    if (rep != NULL) zmq_close(rep);
    if (sub != NULL) zmq_close(sub);
    return NULL;
}

static void descobrir(void)
{
    Rpc r;
    if (rpc(ref->endereco, "list_servers", NULL, 1000, &r) && r.ok) {
        const msgpack_object *lista = buscar(r.payload, "servers");
        if (lista != NULL && lista->type == MSGPACK_OBJECT_ARRAY) {
            Par novos[MAX_SERVIDORES];
            int n = 0;
            for (uint32_t i = 0; i < lista->via.array.size && n < MAX_SERVIDORES; i++)
                if (ler_par(&lista->via.array.ptr[i], &novos[n])) n++;
            pthread_mutex_lock(&trava);
            /* Um servidor novo com rank maior que o coordenador força nova eleição. */
            for (int i = 0; i < n; i++) {
                bool conhecido = false;
                for (int j = 0; j < quantidade; j++)
                    if (strcmp(servidores[j].nome, novos[i].nome) == 0) conhecido = true;
                if (!conhecido && tem_lider && novos[i].rank > lider.rank) eleicao = true;
            }
            memcpy(servidores, novos, sizeof(Par) * (size_t)n);
            quantidade = n;
            pthread_mutex_unlock(&trava);
        }
    }
    rpc_liberar(&r);
}

static void anunciar(void *pub, bool confirmar)
{
    Carga c;
    carga_iniciar(&c);
    eu(&c.pk);
    msgpack_sbuffer evento;
    msgpack_sbuffer_init(&evento);
    envelope(&evento, "coordinator", NULL, &c.buf);
    zmq_send(pub, TOPICO, TAMANHO_TOPICO, ZMQ_SNDMORE);
    zmq_send(pub, evento.data, evento.size, 0);
    printf("[%s] PUB servers | coordenador: %s | rank: %lld\n", ref->nome, ref->nome,
           (long long)ref->rank);
    fflush(stdout);
    if (confirmar) {
        Par lista[MAX_SERVIDORES];
        int n = copiar_servidores(lista);
        for (int i = 0; i < n; i++)
            if (strcmp(lista[i].nome, ref->nome) != 0)
                rpc_ok(lista[i].endereco, "coordinator", &c.buf, 700);
    }
    msgpack_sbuffer_destroy(&evento);
    msgpack_sbuffer_destroy(&c.buf);
}

/* Bully: pede eleição aos de rank maior; se nenhum responder, assume. */
static void eleger(void *pub)
{
    pthread_mutex_lock(&trava);
    eleicao = false;
    tem_lider = false;
    pthread_mutex_unlock(&trava);
    printf("[%s] ELEIÇÃO INICIADA | rank: %lld\n", ref->nome, (long long)ref->rank);
    fflush(stdout);
    Carga c;
    carga_iniciar(&c);
    eu(&c.pk);
    Par lista[MAX_SERVIDORES];
    int n = copiar_servidores(lista);
    bool superior = false;
    for (int i = 0; i < n; i++)
        if (lista[i].rank > ref->rank && rpc_ok(lista[i].endereco, "election", &c.buf, 700))
            superior = true;
    msgpack_sbuffer_destroy(&c.buf);
    if (superior) {
        pthread_mutex_lock(&trava);
        aguardar = agora_monotonico() + 3;
        pthread_mutex_unlock(&trava);
    } else {
        Par e = eu_par();
        if (aceitar(&e)) anunciar(pub, true);
    }
}

/* Berkeley: lê os relógios, calcula a média (coordenador entra com 0) e envia as correções. */
static Rodada berkeley(const char *solicitante)
{
    Rodada res = {.status = "not_coordinator"};
    if (!sou_lider()) return res;
    char rodada[37];
    uuid_t bruto;
    uuid_generate_random(bruto);
    uuid_unparse_lower(bruto, rodada);

    Par lista[MAX_SERVIDORES], part[MAX_SERVIDORES];
    double dif[MAX_SERVIDORES], soma = 0;
    int n = copiar_servidores(lista), np = 0;
    for (int i = 0; i < n; i++) {
        if (strcmp(lista[i].nome, ref->nome) == 0) continue;
        Rpc r;
        if (rpc(lista[i].endereco, "clock_read", NULL, 700, &r) && r.ok) {
            dif[np] = (double)r.timestamp + r.duracao * 500.0 - (double)timestamp_ms();
            soma += dif[np];
            part[np++] = lista[i];
        }
        rpc_liberar(&r);
    }
    if (!sou_lider()) return res;
    double media = soma / (np + 1);
    bool aplicado = strcmp(solicitante, ref->nome) == 0;
    printf("[%s] BERKELEY | média: %.1fms | rodada: %s\n", ref->nome, media, rodada);
    for (int j = 0; j < np; j++) {
        int64_t correcao = llrint(media - dif[j]);
        Carga c;
        carga_iniciar(&c);
        msgpack_pack_map(&c.pk, 4);
        campo_texto(&c.pk, "coordinator", ref->nome);
        campo_int(&c.pk, "rank", ref->rank);
        campo_texto(&c.pk, "round_id", rodada);
        campo_int(&c.pk, "adjustment_ms", correcao);
        bool ok = rpc_ok(part[j].endereco, "clock_adjust", &c.buf, 700);
        msgpack_sbuffer_destroy(&c.buf);
        if (ok && strcmp(part[j].nome, solicitante) == 0) aplicado = true;
        printf("    %s: %lldms %s\n", part[j].nome, (long long)correcao, ok ? "ok" : "sem resposta");
    }
    pthread_mutex_lock(&trava);
    if (!tem_lider || strcmp(lider.nome, ref->nome) != 0) {
        pthread_mutex_unlock(&trava);
        return res;
    }
    relogio_aplicar_ajuste(llrint(media));
    strcpy(ultima_rodada, rodada);
    rodadas++;
    pthread_mutex_unlock(&trava);
    printf("    %s (coordenador): %lldms\n", ref->nome, (long long)llrint(media));
    fflush(stdout);
    res.status = aplicado ? "ok" : "sync_incomplete";
    strcpy(res.rodada, rodada);
    res.aplicado = aplicado;
    return res;
}

static void *trabalhar(void *arg)
{
    (void)arg;
    int zero = 0;
    void *pub = zmq_socket(contexto, ZMQ_PUB);
    zmq_setsockopt(pub, ZMQ_LINGER, &zero, sizeof zero);
    zmq_connect(pub, ambiente("ENDERECO_PUBLICACAO", "tcp://localhost:5557"));
    double descobrir_em = 0, verificar_em = agora_monotonico() + 1, anunciar_em = 0;
    while (!parar) {
        if (ref->rank == 0) {
            dormir(0.05);
            continue;
        }
        double t = agora_monotonico();
        if (t >= descobrir_em) {
            descobrir();
            descobrir_em = agora_monotonico() + 2;
        }
        pthread_mutex_lock(&trava);
        bool deve_eleger = eleicao || (!tem_lider && t >= aguardar);
        pthread_mutex_unlock(&trava);
        if (deve_eleger) eleger(pub);

        pthread_mutex_lock(&trava);
        bool tem = tem_lider;
        Par l = lider;
        pthread_mutex_unlock(&trava);
        bool sou = tem && strcmp(l.nome, ref->nome) == 0;
        if (sou && t >= anunciar_em) {
            anunciar(pub, false);
            anunciar_em = agora_monotonico() + 2;
        } else if (tem && !sou && t >= verificar_em) {
            if (!rpc_ok(l.endereco, "coordinator_status", NULL, 3000)) perder(&l);
            verificar_em = agora_monotonico() + 2;
        }

        /* clock_sync pedido por outro servidor. */
        char nome[65];
        pthread_mutex_lock(&trava);
        bool atender_sync = sync_pedido && !sync_pronto;
        strcpy(nome, sync_nome);
        pthread_mutex_unlock(&trava);
        if (atender_sync) {
            Rodada r = berkeley(nome);
            pthread_mutex_lock(&trava);
            sync_resultado = r;
            sync_pronto = true;
            pthread_cond_broadcast(&sinal);
            pthread_mutex_unlock(&trava);
        }

        /* A cada 15 mensagens: o coordenador roda Berkeley; os outros pedem clock_sync. */
        pthread_mutex_lock(&trava);
        bool sincronizar = pendentes > 0 && tem_lider;
        if (sincronizar) pendentes--;
        l = lider;
        pthread_mutex_unlock(&trava);
        if (sincronizar) {
            if (strcmp(l.nome, ref->nome) == 0) {
                berkeley(ref->nome);
            } else {
                Carga c;
                carga_iniciar(&c);
                msgpack_pack_map(&c.pk, 1);
                campo_texto(&c.pk, "name", ref->nome);
                Rpc r;
                if (!rpc(l.endereco, "clock_sync", &c.buf, 8000, &r) ||
                    texto_igual(r.status, "not_coordinator")) {
                    perder(&l);
                } else {
                    printf("[%s] SINCRONIZAÇÃO | coordenador: %s | status: %.*s\n", ref->nome,
                           l.nome, (int)r.status->via.str.size, r.status->via.str.ptr);
                    fflush(stdout);
                }
                rpc_liberar(&r);
                msgpack_sbuffer_destroy(&c.buf);
            }
        }
        dormir(0.03);
    }
    zmq_close(pub);
    return NULL;
}

int coordenacao_iniciar(void *ctx, Referencia *referencia)
{
    contexto = ctx;
    ref = referencia;
    parar = false;
    pronto = 0;
    /* Os sinais ficam com a thread principal. */
    sigset_t bloqueio, anterior;
    sigemptyset(&bloqueio);
    sigaddset(&bloqueio, SIGINT);
    sigaddset(&bloqueio, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &bloqueio, &anterior);
    int erro = pthread_create(&ouvinte, NULL, ouvir, NULL);
    pthread_sigmask(SIG_SETMASK, &anterior, NULL);
    if (erro != 0) return -1;

    struct timespec limite;
    clock_gettime(CLOCK_REALTIME, &limite);
    limite.tv_sec += 3;
    pthread_mutex_lock(&trava);
    while (pronto == 0)
        if (pthread_cond_timedwait(&sinal, &trava, &limite) == ETIMEDOUT) break;
    int estado = pronto;
    pthread_mutex_unlock(&trava);
    if (estado != 1) {
        parar = true;
        pthread_join(ouvinte, NULL);
        return -1;
    }

    pthread_sigmask(SIG_BLOCK, &bloqueio, &anterior);
    erro = pthread_create(&trabalhador, NULL, trabalhar, NULL);
    pthread_sigmask(SIG_SETMASK, &anterior, NULL);
    if (erro != 0) {
        parar = true;
        pthread_join(ouvinte, NULL);
        return -1;
    }
    iniciado = true;
    return 0;
}

/* Conta REQ recebido, REP enviado e PUB de canal; a cada 15, agenda uma sincronização. */
void coordenacao_contar_mensagem(void)
{
    pthread_mutex_lock(&trava);
    if (++mensagens >= 15) {
        mensagens -= 15;
        pendentes++;
    }
    pthread_mutex_unlock(&trava);
}

void coordenacao_encerrar(void)
{
    if (!iniciado) return;
    parar = true;
    pthread_mutex_lock(&trava);
    pthread_cond_broadcast(&sinal);
    pthread_mutex_unlock(&trava);
    pthread_join(ouvinte, NULL);
    pthread_join(trabalhador, NULL);
    iniciado = false;
}
