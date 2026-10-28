#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <msgpack.h>
#include <zmq.h>

#include "protocolo.h"

#define TEMPO_RESPOSTA_MS 5000
#define INTERVALO_TENTATIVAS 2.0
#define INTERVALO_MENSAGENS 1.0
#define MAXIMO_CANAIS 5
#define MAXIMO_ASSINATURAS 3
#define MENSAGENS_POR_LOTE 10

static const char *frases[] = {
    "Bom dia, pessoal!",          "Alguém está por aqui?", "Teste de publicação.",
    "Mensagem enviada pelo bot.", "Tudo certo por aqui!",  "Olá, canal!",
};

/* Avisado pelo tratador de sinal; as esperas e os laços conferem esta marca. */
static volatile sig_atomic_t parar = 0;

static char nome_bot[256];
static const char *endereco_broker;
static const char *endereco_inscricao;

/* Lista de nomes de canais. Guardamos o tamanho porque uma string
 * MessagePack pode, em tese, conter bytes nulos. */
typedef struct {
    char *texto;
    size_t tamanho;
} Nome;

typedef struct {
    Nome *itens;
    size_t quantidade;
    size_t capacidade;
} Lista;


typedef struct {
    zmq_msg_t mensagem;
    msgpack_unpacked objeto;
    const msgpack_object *status;
    const msgpack_object *payload;
} Resposta;

typedef struct {
    const char *chave;
    const char *valor;
    size_t tamanho;
} Campo;

static void tratar_sinal(int sinal)
{
    (void)sinal;
    parar = 1;
}

static void liberar_lista(Lista *lista)
{
    for (size_t i = 0; i < lista->quantidade; i++) {
        free(lista->itens[i].texto);
    }
    free(lista->itens);
    lista->itens = NULL;
    lista->quantidade = 0;
    lista->capacidade = 0;
}

static bool lista_contem(const Lista *lista, const char *texto, size_t tamanho)
{
    for (size_t i = 0; i < lista->quantidade; i++) {
        if (lista->itens[i].tamanho == tamanho && memcmp(lista->itens[i].texto, texto, tamanho) == 0) {
            return true;
        }
    }
    return false;
}

static int lista_adicionar(Lista *lista, const char *texto, size_t tamanho)
{
    if (lista->quantidade == lista->capacidade) {
        size_t nova = lista->capacidade ? lista->capacidade * 2 : 8;
        Nome *maior = realloc(lista->itens, nova * sizeof *maior);
        if (maior == NULL) {
            return -1;
        }
        lista->itens = maior;
        lista->capacidade = nova;
    }
    char *copia = malloc(tamanho + 1);
    if (copia == NULL) {
        return -1;
    }
    memcpy(copia, texto, tamanho);
    copia[tamanho] = '\0';
    lista->itens[lista->quantidade].texto = copia;
    lista->itens[lista->quantidade].tamanho = tamanho;
    lista->quantidade++;
    return 0;
}

static int comparar_nomes(const void *a, const void *b)
{
    const Nome *x = a;
    const Nome *y = b;
    size_t menor = x->tamanho < y->tamanho ? x->tamanho : y->tamanho;
    int resultado = memcmp(x->texto, y->texto, menor);
    if (resultado != 0) {
        return resultado;
    }
    return (x->tamanho > y->tamanho) - (x->tamanho < y->tamanho);
}

static size_t sortear(size_t quantidade)
{
    return (size_t)rand() % quantidade;
}

static void mostrar_mensagem(const char *acao, const msgpack_object *mensagem)
{
    static const char *campos[][2] = {
        {"username", "usuário"}, {"channel", "canal"}, {"message", "mensagem"}};
    const msgpack_object *operacao = buscar(mensagem, "operation");
    const msgpack_object *instante = buscar(mensagem, "timestamp");
    const msgpack_object *dados = buscar(mensagem, "payload");
    const msgpack_object *status = buscar(mensagem, "status");

    printf("[%s] %s | ", nome_bot, acao);
    if (operacao != NULL) {
        imprimir_objeto(stdout, operacao, false);
    } else {
        fputs("desconhecida", stdout);
    }
    fputs(" | timestamp: ", stdout);
    if (instante != NULL) {
        imprimir_objeto(stdout, instante, false);
    } else {
        fputs("None", stdout);
    }
    for (size_t i = 0; i < sizeof campos / sizeof campos[0]; i++) {
        const msgpack_object *valor = buscar(dados, campos[i][0]);
        if (valor != NULL) {
            printf(" | %s: ", campos[i][1]);
            imprimir_objeto(stdout, valor, false);
        }
    }
    const msgpack_object *canais = buscar(dados, "channels");
    if (canais != NULL && canais->type == MSGPACK_OBJECT_ARRAY) {
        fputs(" | canais: ", stdout);
        if (canais->via.array.size == 0) {
            fputs("nenhum", stdout);
        }
        for (uint32_t i = 0; i < canais->via.array.size; i++) {
            if (i > 0) {
                fputs(", ", stdout);
            }
            imprimir_objeto(stdout, &canais->via.array.ptr[i], false);
        }
    }
    if (status != NULL) {
        fputs(" | status: ", stdout);
        imprimir_objeto(stdout, status, false);
    }
    fputc('\n', stdout);
    if (mensagens_completas()) {
        fputs("    conteúdo: ", stdout);
        imprimir_objeto(stdout, mensagem, true);
        fputc('\n', stdout);
    }
    fflush(stdout);
}


static bool mostrar_publicacao(zmq_msg_t *frames, int64_t recebido_em)
{
    bool valida = false;
    msgpack_unpacked topico;
    msgpack_unpacked evento;
    msgpack_unpacked_init(&topico);
    msgpack_unpacked_init(&evento);
    if (decodificar(zmq_msg_data(&frames[0]), zmq_msg_size(&frames[0]), &topico) == 0 &&
        decodificar(zmq_msg_data(&frames[1]), zmq_msg_size(&frames[1]), &evento) == 0) {
        const msgpack_object *canal = &topico.data;
        const msgpack_object *instante = buscar(&evento.data, "timestamp");
        const msgpack_object *dados = buscar(&evento.data, "payload");
        const msgpack_object *canal_evento = buscar(dados, "channel");
        const msgpack_object *usuario = buscar(dados, "username");
        const msgpack_object *texto = buscar(dados, "message");
        valida = texto_igual(buscar(&evento.data, "operation"), "publication") &&
                 canal->type == MSGPACK_OBJECT_STR && canal_evento != NULL &&
                 canal_evento->type == MSGPACK_OBJECT_STR &&
                 canal->via.str.size == canal_evento->via.str.size &&
                 memcmp(canal->via.str.ptr, canal_evento->via.str.ptr, canal->via.str.size) == 0 &&
                 eh_inteiro(instante) && usuario != NULL && texto != NULL &&
                 texto->type == MSGPACK_OBJECT_STR;
        if (valida) {
            printf("[%s] PUBLICAÇÃO | canal: ", nome_bot);
            imprimir_objeto(stdout, canal, false);
            fputs(" | usuário: ", stdout);
            imprimir_objeto(stdout, usuario, false);
            fputs(" | mensagem: ", stdout);
            imprimir_objeto(stdout, texto, false);
            fputs(" | envio: ", stdout);
            imprimir_objeto(stdout, instante, false);
            printf(" | recebimento: %lld\n", (long long)recebido_em);
            if (mensagens_completas()) {
                fputs("    conteúdo: ", stdout);
                imprimir_objeto(stdout, &evento.data, true);
                fputc('\n', stdout);
            }
            fflush(stdout);
        }
    }
    msgpack_unpacked_destroy(&topico);
    msgpack_unpacked_destroy(&evento);
    return valida;
}

static void receber_publicacoes(void *inscrito)
{
 
    for (int vez = 0; vez < 100; vez++) {
        zmq_msg_t frames[2];
        zmq_msg_init(&frames[0]);
        zmq_msg_init(&frames[1]);
        if (zmq_msg_recv(&frames[0], inscrito, ZMQ_DONTWAIT) < 0) {
            zmq_msg_close(&frames[0]);
            zmq_msg_close(&frames[1]);
            return;
        }
        int64_t recebido_em = timestamp_ms();
        int quantidade = 1;
        bool falhou = false;
        int mais = zmq_msg_more(&frames[0]);
        while (mais) {
            zmq_msg_t extra;
            zmq_msg_t *destino = quantidade < 2 ? &frames[1] : &extra;
            if (destino == &extra) {
                zmq_msg_init(&extra);
            }
            if (zmq_msg_recv(destino, inscrito, 0) < 0) {
                if (destino == &extra) {
                    zmq_msg_close(&extra);
                }
                if (zmq_errno() == EINTR) {
                    continue;
                }
                falhou = true;
                break;
            }
            quantidade++;
            mais = zmq_msg_more(destino);
            if (destino == &extra) {
                zmq_msg_close(&extra);
            }
        }
        if (falhou || quantidade != 2 || !mostrar_publicacao(frames, recebido_em)) {
            printf("[%s] Publicação inválida recebida.\n", nome_bot);
            fflush(stdout);
        }
        zmq_msg_close(&frames[0]);
        zmq_msg_close(&frames[1]);
    }
}

static void esperar(void *inscrito, double segundos)
{
    double fim = agora_monotonico() + segundos;
    while (!parar) {
        receber_publicacoes(inscrito);
        double restante = fim - agora_monotonico();
        if (restante <= 0) {
            return;
        }
        long passo = (long)(restante * 1000);
        if (passo < 1) {
            passo = 1;
        } else if (passo > 100) {
            passo = 100;
        }
        zmq_pollitem_t item = {inscrito, 0, ZMQ_POLLIN, 0};
        zmq_poll(&item, 1, passo);
    }
}

static void liberar_resposta(Resposta *resposta)
{
    msgpack_unpacked_destroy(&resposta->objeto);
    zmq_msg_close(&resposta->mensagem);
}

static int montar_pedido(const char *operacao, const Campo *campos, size_t quantidade,
                         msgpack_sbuffer *buffer)
{
    msgpack_packer pk;
    int erro = 0;
    msgpack_packer_init(&pk, buffer, msgpack_sbuffer_write);
    erro |= msgpack_pack_map(&pk, 3);
    erro |= empacotar_texto(&pk, "operation", 9);
    erro |= empacotar_texto(&pk, operacao, strlen(operacao));
    erro |= empacotar_texto(&pk, "timestamp", 9);
    erro |= msgpack_pack_int64(&pk, timestamp_ms());
    erro |= empacotar_texto(&pk, "payload", 7);
    erro |= msgpack_pack_map(&pk, quantidade);
    for (size_t i = 0; i < quantidade; i++) {
        erro |= empacotar_texto(&pk, campos[i].chave, strlen(campos[i].chave));
        erro |= empacotar_texto(&pk, campos[i].valor, campos[i].tamanho);
    }
    return erro ? -1 : 0;
}

static bool resposta_valida(const char *operacao, Resposta *resposta)
{
    const msgpack_object *objeto = &resposta->objeto.data;
    if (objeto->type != MSGPACK_OBJECT_MAP || !texto_igual(buscar(objeto, "operation"), operacao) ||
        !eh_inteiro(buscar(objeto, "timestamp"))) {
        return false;
    }
    resposta->status = buscar(objeto, "status");
    resposta->payload = buscar(objeto, "payload");
    return resposta->status != NULL && resposta->status->type == MSGPACK_OBJECT_STR &&
           resposta->payload != NULL && resposta->payload->type == MSGPACK_OBJECT_MAP;
}

static int fazer_pedido(void *contexto, void *inscrito, const char *operacao,
                        const Campo *campos, size_t quantidade, Resposta *resposta)
{
    int resultado = -1;
    bool esgotado = true;
    msgpack_sbuffer pedido;
    msgpack_sbuffer_init(&pedido);
    msgpack_unpacked_init(&resposta->objeto);
    zmq_msg_init(&resposta->mensagem);

    void *socket = zmq_socket(contexto, ZMQ_REQ);
    if (socket == NULL) {
        printf("[%s] Erro ao criar o socket: %s\n", nome_bot, zmq_strerror(zmq_errno()));
        goto fim;
    }
    int zero = 0;
    int limite = TEMPO_RESPOSTA_MS;
    if (zmq_setsockopt(socket, ZMQ_LINGER, &zero, sizeof zero) != 0 ||
        zmq_setsockopt(socket, ZMQ_SNDTIMEO, &limite, sizeof limite) != 0 ||
        zmq_connect(socket, endereco_broker) != 0 ||
        montar_pedido(operacao, campos, quantidade, &pedido) != 0) {
        printf("[%s] Não foi possível concluir o pedido.\n", nome_bot);
        goto fim;
    }

    msgpack_unpacked enviado;
    msgpack_unpacked_init(&enviado);
    if (decodificar(pedido.data, pedido.size, &enviado) == 0) {
        mostrar_mensagem("ENVIANDO", &enviado.data);
    }
    msgpack_unpacked_destroy(&enviado);

    if (zmq_send(socket, pedido.data, pedido.size, 0) < 0) {
        printf("[%s] Não foi possível concluir o pedido.\n", nome_bot);
        goto fim;
    }
    double prazo = agora_monotonico() + TEMPO_RESPOSTA_MS / 1000.0;
    while (!parar) {
        double restante = prazo - agora_monotonico();
        if (restante <= 0) {
            break;
        }
        receber_publicacoes(inscrito);
        long passo = (long)(restante * 1000);
        if (passo > 50) {
            passo = 50;
        }
        zmq_pollitem_t itens[2] = {{socket, 0, ZMQ_POLLIN, 0}, {inscrito, 0, ZMQ_POLLIN, 0}};
        if (zmq_poll(itens, 2, passo) < 0) {
            continue;
        }
        if (itens[0].revents & ZMQ_POLLIN) {
            esgotado = false;
            if (zmq_msg_recv(&resposta->mensagem, socket, 0) < 0 ||
                decodificar(zmq_msg_data(&resposta->mensagem), zmq_msg_size(&resposta->mensagem),
                            &resposta->objeto) != 0 ||
                !resposta_valida(operacao, resposta)) {
                printf("[%s] Não foi possível concluir o pedido.\n", nome_bot);
                break;
            }
            mostrar_mensagem("RECEBENDO", &resposta->objeto.data);
            resultado = 0;
            break;
        }
    }
    if (esgotado && !parar) {
        printf("[%s] Tempo de resposta esgotado.\n", nome_bot);
    }

fim:
    fflush(stdout);
    if (socket != NULL) {
        zmq_close(socket);
    }
    msgpack_sbuffer_destroy(&pedido);
    if (resultado != 0) {
        liberar_resposta(resposta);
    }
    return resultado;
}


static int listar_canais(void *contexto, void *inscrito, Lista *canais)
{
    Resposta resposta;
    int resultado = -1;
    if (fazer_pedido(contexto, inscrito, "list_channels", NULL, 0, &resposta) != 0) {
        return -1;
    }
    const msgpack_object *lista = buscar(resposta.payload, "channels");
    if (texto_igual(resposta.status, "ok") && lista != NULL && lista->type == MSGPACK_OBJECT_ARRAY) {
        resultado = 0;
        for (uint32_t i = 0; i < lista->via.array.size; i++) {
            if (lista->via.array.ptr[i].type != MSGPACK_OBJECT_STR) {
                resultado = -1;
                break;
            }
        }
        for (uint32_t i = 0; resultado == 0 && i < lista->via.array.size; i++) {
            const msgpack_object *nome = &lista->via.array.ptr[i];
            if (lista_adicionar(canais, nome->via.str.ptr, nome->via.str.size) != 0) {
                resultado = -1;
            }
        }
        if (resultado != 0) {
            liberar_lista(canais);
        }
    }
    liberar_resposta(&resposta);
    return resultado;
}


static bool numero_do_canal(const Nome *nome, unsigned long long *numero)
{
    if (nome->tamanho <= 5 || memcmp(nome->texto, "canal", 5) != 0) {
        return false;
    }
    unsigned long long valor = 0;
    for (size_t i = 5; i < nome->tamanho; i++) {
        char c = nome->texto[i];
        if (c < '0' || c > '9') {
            return false;
        }
        unsigned digito = (unsigned)(c - '0');
        if (valor > (ULLONG_MAX - digito) / 10) {
            return false;
        }
        valor = valor * 10 + digito;
    }
    *numero = valor;
    return true;
}

static void preparar_canais(void *contexto, void *inscrito, Lista *assinaturas, Lista *canais)
{
    liberar_lista(canais);
    if (listar_canais(contexto, inscrito, canais) != 0) {
        return;
    }
    if (canais->quantidade < MAXIMO_CANAIS) {
        /* Um nome novo por passagem, até que a lista tenha pelo menos cinco. */
        unsigned long long maior = 0;
        for (size_t i = 0; i < canais->quantidade; i++) {
            unsigned long long numero;
            if (numero_do_canal(&canais->itens[i], &numero) && numero > maior) {
                maior = numero;
            }
        }
        char novo[64];
        int tamanho = snprintf(novo, sizeof novo, "canal%llu", maior + 1);
        Campo campos[] = {
            {"username", nome_bot, strlen(nome_bot)},
            {"channel", novo, (size_t)tamanho},
        };
        Resposta criacao;
        if (tamanho > 0 && (size_t)tamanho < sizeof novo &&
            fazer_pedido(contexto, inscrito, "create_channel", campos, 2, &criacao) == 0) {
            /* Só adicionamos à lista um canal que o servidor confirmou. */
            if ((texto_igual(criacao.status, "ok") || texto_igual(criacao.status, "channel_exists")) &&
                !lista_contem(canais, novo, (size_t)tamanho) &&
                lista_adicionar(canais, novo, (size_t)tamanho) == 0) {
                qsort(canais->itens, canais->quantidade, sizeof *canais->itens, comparar_nomes);
            }
            liberar_resposta(&criacao);
        }
    }
    if (assinaturas->quantidade < MAXIMO_ASSINATURAS && !parar) {
        /* Sorteamos entre os canais listados que ainda não foram assinados. */
        size_t quantidade = 0;
        for (size_t i = 0; i < canais->quantidade; i++) {
            if (!lista_contem(assinaturas, canais->itens[i].texto, canais->itens[i].tamanho)) {
                quantidade++;
            }
        }
        if (quantidade > 0) {
            size_t sorteado = sortear(quantidade);
            const Nome *canal = NULL;
            for (size_t i = 0; canal == NULL; i++) {
                if (!lista_contem(assinaturas, canais->itens[i].texto, canais->itens[i].tamanho) &&
                    sorteado-- == 0) {
                    canal = &canais->itens[i];
                }
            }
            /* O tópico também usa MessagePack, como o primeiro frame do PUB. */
            msgpack_sbuffer topico;
            msgpack_packer pk;
            msgpack_sbuffer_init(&topico);
            msgpack_packer_init(&pk, &topico, msgpack_sbuffer_write);
            if (empacotar_texto(&pk, canal->texto, canal->tamanho) == 0 &&
                zmq_setsockopt(inscrito, ZMQ_SUBSCRIBE, topico.data, topico.size) == 0 &&
                lista_adicionar(assinaturas, canal->texto, canal->tamanho) == 0) {
                printf("[%s] INSCRITO | canal: %s\n", nome_bot, canal->texto);
                fflush(stdout);
                /* Damos tempo para a inscrição atravessar o proxy (slow joiner). */
                esperar(inscrito, 1);
            }
            msgpack_sbuffer_destroy(&topico);
        }
    }
}

static void publicar_lote(void *contexto, void *inscrito, const Nome *canal)
{
    for (int numero = 0; numero < MENSAGENS_POR_LOTE && !parar; numero++) {
        double inicio = agora_monotonico();
        const char *frase = frases[sortear(sizeof frases / sizeof frases[0])];
        Campo campos[] = {
            {"username", nome_bot, strlen(nome_bot)},
            {"channel", canal->texto, canal->tamanho},
            {"message", frase, strlen(frase)},
        };
        Resposta resposta;
        if (fazer_pedido(contexto, inscrito, "publish", campos, 3, &resposta) == 0) {
            liberar_resposta(&resposta);
        }
        /* O tempo do pedido entra na conta: evitamos somar mais 1 segundo.
         * Se ele demorar mais de 1 segundo, não tentamos compensar com rajadas.
         * Mantemos o intervalo também entre o fim de um lote e o próximo. */
        double restante = INTERVALO_MENSAGENS - (agora_monotonico() - inicio);
        esperar(inscrito, restante > 0 ? restante : 0);
    }
}

int main(void)
{
    endereco_broker = ambiente("ENDERECO_BROKER", "tcp://localhost:5555");
    endereco_inscricao = ambiente("ENDERECO_INSCRICAO", "tcp://localhost:5558");
    char nome_padrao[64];
    snprintf(nome_padrao, sizeof nome_padrao, "bot-c-%.12s", ambiente("HOSTNAME", "local"));
    snprintf(nome_bot, sizeof nome_bot, "%s", ambiente("NOME_BOT", nome_padrao));
    /* Tempo e pid juntos evitam que bots iniciados no mesmo segundo repitam
     * a mesma sequência de canais e frases. */
    srand((unsigned)time(NULL) ^ ((unsigned)getpid() << 16));

    /* Sem SA_RESTART, as esperas do ZeroMQ retornam com EINTR e o bot encerra. */
    struct sigaction acao;
    memset(&acao, 0, sizeof acao);
    acao.sa_handler = tratar_sinal;
    sigemptyset(&acao.sa_mask);
    sigaction(SIGINT, &acao, NULL);
    sigaction(SIGTERM, &acao, NULL);

    void *contexto = zmq_ctx_new();
    if (contexto == NULL) {
        printf("Erro ao criar o contexto ZeroMQ: %s\n", zmq_strerror(zmq_errno()));
        return 1;
    }
    int zero = 0;
    void *inscrito = zmq_socket(contexto, ZMQ_SUB);
    if (inscrito == NULL || zmq_setsockopt(inscrito, ZMQ_LINGER, &zero, sizeof zero) != 0 ||
        zmq_connect(inscrito, endereco_inscricao) != 0) {
        printf("Erro ao conectar: %s\n", zmq_strerror(zmq_errno()));
        if (inscrito != NULL) {
            zmq_close(inscrito);
        }
        zmq_ctx_term(contexto);
        return 1;
    }

    Lista assinaturas = {0};
    Lista canais = {0};
    Campo login[] = {{"username", nome_bot, strlen(nome_bot)}};
    while (!parar) {
        Resposta resposta;
        if (fazer_pedido(contexto, inscrito, "login", login, 1, &resposta) == 0) {
            bool aceito = texto_igual(resposta.status, "ok");
            liberar_resposta(&resposta);
            if (aceito) {
                break;
            }
        }
        esperar(inscrito, INTERVALO_TENTATIVAS);
    }

    /* Fazemos três passagens iniciais para começar com até três inscrições.
     * Se faltarem canais ou houver falha, completamos nas próximas rodadas. */
    for (int i = 0; i < 3 && !parar; i++) {
        preparar_canais(contexto, inscrito, &assinaturas, &canais);
    }

    while (!parar) {
        preparar_canais(contexto, inscrito, &assinaturas, &canais);
        if (canais.quantidade == 0) {
            esperar(inscrito, INTERVALO_TENTATIVAS);
            continue;
        }
        /* Escolhemos um canal por lote, mesmo que não estejamos inscritos nele. */
        publicar_lote(contexto, inscrito, &canais.itens[sortear(canais.quantidade)]);
        receber_publicacoes(inscrito);
    }

    printf("Bot encerrado.\n");
    fflush(stdout);
    liberar_lista(&canais);
    liberar_lista(&assinaturas);
    zmq_close(inscrito);
    zmq_ctx_term(contexto);
    return 0;
}
