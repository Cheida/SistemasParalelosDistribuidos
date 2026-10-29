/* Recebe pedidos em MessagePack e salva logins, canais e publicações em SQLite.
 * Versão em C do python/servidor.py. Compile na pasta C:
 *   gcc -std=c11 -Wall -Wextra -o servidor servidor.c protocolo.c \
 *       -lzmq -lmsgpackc -lsqlite3 -luuid
 */
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <math.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include <msgpack.h>
#include <sqlite3.h>
#include <uuid/uuid.h>
#include <zmq.h>

#include "protocolo.h"
#include "referencia.h"

/* Limite de caracteres (não de bytes) do texto publicado. */
#define TAMANHO_MAXIMO_MENSAGEM 500

/* Avisado pelo tratador de sinal; o laço principal confere a cada pedido. */
static volatile sig_atomic_t parar = 0;

static char nome_servidor[256];
static double pausa_visual = 0;

/* Comandos preparados uma vez e reutilizados em todos os pedidos. Os valores
 * entram sempre por bind, sem montar SQL com os dados recebidos. */
typedef struct {
    sqlite3 *conexao;
    sqlite3_stmt *inserir_login;
    sqlite3_stmt *inserir_canal;
    sqlite3_stmt *listar_canais;
    sqlite3_stmt *inserir_publicacao;
    sqlite3_stmt *inserir_troca;
} Banco;

/* Formatos possíveis do payload de uma resposta. */
typedef enum {
    DADOS_VAZIO,
    DADOS_USUARIO,
    DADOS_CANAL,
    DADOS_PUBLICACAO,
    DADOS_CANAIS
} TipoDados;

typedef struct {
    char *texto;
    size_t tamanho;
} Texto;

typedef struct {
    /* A operação aponta para os bytes do pedido, que vivem até a resposta. */
    const char *operacao;
    size_t tamanho_operacao;
    const char *status;
    TipoDados tipo;
    /* Usuário ou canal devolvido no payload, também apontando para o pedido. */
    const char *texto;
    size_t tamanho_texto;
    char id[37];
    Texto *canais;
    size_t quantidade_canais;
} Resposta;

static void tratar_sinal(int sinal)
{
    (void)sinal;
    parar = 1;
}

static void liberar_canais(Resposta *resposta)
{
    for (size_t i = 0; i < resposta->quantidade_canais; i++) {
        free(resposta->canais[i].texto);
    }
    free(resposta->canais);
    resposta->canais = NULL;
    resposta->quantidade_canais = 0;
}

/* Em caso de falha, o status informa o motivo e o payload volta vazio. */
static void definir_erro(Resposta *resposta, const char *status)
{
    liberar_canais(resposta);
    resposta->status = status;
    resposta->tipo = DADOS_VAZIO;
}

static void mostrar_mensagem(const char *acao, const msgpack_object *mensagem)
{
    /* Exibimos o timestamp transmitido, junto da operação e dos dados. */
    if (mensagem == NULL || mensagem->type != MSGPACK_OBJECT_MAP) {
        printf("[%s] %s | mensagem inválida\n", nome_servidor, acao);
        fflush(stdout);
        return;
    }

    static const char *nomes[][2] = {
        {"login", "login"},
        {"list_channels", "listar canais"},
        {"create_channel", "criar canal"},
        {"publish", "publicar"},
    };
    const msgpack_object *operacao = buscar(mensagem, "operation");
    const msgpack_object *clock = buscar(mensagem, "clock");
    const msgpack_object *instante = buscar(mensagem, "timestamp");
    const msgpack_object *dados = buscar(mensagem, "payload");
    const msgpack_object *status = buscar(mensagem, "status");

    printf("[%s] %s | ", nome_servidor, acao);
    if (operacao == NULL) {
        fputs("desconhecida", stdout);
    } else {
        const char *traduzida = NULL;
        for (size_t i = 0; i < sizeof nomes / sizeof nomes[0]; i++) {
            if (texto_igual(operacao, nomes[i][0])) {
                traduzida = nomes[i][1];
            }
        }
        if (traduzida != NULL) {
            fputs(traduzida, stdout);
        } else {
            imprimir_objeto(stdout, operacao, false);
        }
    }
    fputs(" | clock: ", stdout);
    if (clock != NULL) imprimir_objeto(stdout, clock, false);
    else fputs("ausente", stdout);
    fputs(" | timestamp: ", stdout);
    if (instante == NULL) {
        fputs("ausente", stdout);
    } else {
        imprimir_objeto(stdout, instante, false);
    }

    /* Sem payload, o Python usa {} como padrão e mostra "dados: {}". */
    if (dados == NULL || (dados->type == MSGPACK_OBJECT_MAP && dados->via.map.size == 0)) {
        fputs(" | dados: {}", stdout);
    } else if (dados->type == MSGPACK_OBJECT_MAP) {
        static const char *campos[][2] = {{"username", "usuário"}, {"channel", "canal"}};
        for (size_t i = 0; i < 2; i++) {
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
        const msgpack_object *texto = buscar(dados, "message");
        if (texto != NULL) {
            fputs(" | mensagem: ", stdout);
            imprimir_objeto(stdout, texto, false);
        }
    }
    if (status != NULL) {
        fputs(" | status: ", stdout);
        imprimir_objeto(stdout, status, false);
    }
    fputc('\n', stdout);

    /* Para conferir todos os campos na apresentação, ative MENSAGENS_COMPLETAS=1. */
    if (mensagens_completas()) {
        fputs("    conteúdo: ", stdout);
        imprimir_objeto(stdout, mensagem, true);
        fputc('\n', stdout);
    }
    fflush(stdout);
}

/* Mostra uma mensagem que nós mesmos empacotamos (resposta ou evento). */
static void mostrar_empacotada(const char *acao, const char *dados, size_t tamanho)
{
    msgpack_unpacked objeto;
    msgpack_unpacked_init(&objeto);
    if (decodificar(dados, tamanho, &objeto) == 0) {
        mostrar_mensagem(acao, &objeto.data);
    } else {
        mostrar_mensagem(acao, NULL);
    }
    msgpack_unpacked_destroy(&objeto);
}

/* Mostra os bytes recusados como o repr de bytes do Python. */
static void mostrar_bytes_invalidos(const char *dados, size_t tamanho)
{
    msgpack_object bytes;
    bytes.type = MSGPACK_OBJECT_BIN;
    bytes.via.bin.ptr = dados;
    bytes.via.bin.size = (uint32_t)(tamanho > UINT32_MAX ? UINT32_MAX : tamanho);
    fputs("Mensagem inválida: ", stdout);
    imprimir_objeto(stdout, &bytes, true);
    fputc('\n', stdout);
    fflush(stdout);
}

static void pausar(double segundos)
{
    if (segundos <= 0) {
        return;
    }
    struct timespec espera;
    espera.tv_sec = (time_t)segundos;
    espera.tv_nsec = (long)((segundos - (double)espera.tv_sec) * 1e9);
    nanosleep(&espera, NULL);
}

static bool caractere_nome(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
           c == '_' || c == '-';
}

/* O nome inteiro precisa ter de 1 a 32 caracteres de [A-Za-z0-9_-]. */
static bool nome_valido(const msgpack_object *nome)
{
    if (nome->type != MSGPACK_OBJECT_STR || nome->via.str.size < 1 || nome->via.str.size > 32) {
        return false;
    }
    for (uint32_t i = 0; i < nome->via.str.size; i++) {
        if (!caractere_nome(nome->via.str.ptr[i])) {
            return false;
        }
    }
    return true;
}

/* Mesmo conjunto de espaços que o str.isspace() do Python reconhece. */
static bool eh_espaco(uint32_t ponto)
{
    return (ponto >= 0x09 && ponto <= 0x0D) || (ponto >= 0x1C && ponto <= 0x20) ||
           ponto == 0x85 || ponto == 0xA0 || ponto == 0x1680 ||
           (ponto >= 0x2000 && ponto <= 0x200A) || ponto == 0x2028 || ponto == 0x2029 ||
           ponto == 0x202F || ponto == 0x205F || ponto == 0x3000;
}

/* O texto precisa ter de 1 a 500 caracteres e não ser só espaços. Contamos
 * caracteres Unicode, como o len() do Python, e não bytes UTF-8. O UTF-8 já
 * foi validado na decodificação. */
static bool mensagem_valida(const char *texto, size_t tamanho)
{
    const unsigned char *p = (const unsigned char *)texto;
    size_t caracteres = 0;
    bool so_espacos = true;
    size_t i = 0;
    while (i < tamanho) {
        uint32_t ponto;
        size_t largura;
        if (p[i] < 0x80) {
            ponto = p[i];
            largura = 1;
        } else if (p[i] < 0xE0) {
            ponto = ((uint32_t)(p[i] & 0x1F) << 6) | (p[i + 1] & 0x3F);
            largura = 2;
        } else if (p[i] < 0xF0) {
            ponto = ((uint32_t)(p[i] & 0x0F) << 12) | ((uint32_t)(p[i + 1] & 0x3F) << 6) |
                    (p[i + 2] & 0x3F);
            largura = 3;
        } else {
            ponto = ((uint32_t)(p[i] & 0x07) << 18) | ((uint32_t)(p[i + 1] & 0x3F) << 12) |
                    ((uint32_t)(p[i + 2] & 0x3F) << 6) | (p[i + 3] & 0x3F);
            largura = 4;
        }
        if (!eh_espaco(ponto)) {
            so_espacos = false;
        }
        caracteres++;
        i += largura;
    }
    return !so_espacos && caracteres <= TAMANHO_MAXIMO_MENSAGEM;
}

/* Executa um comando preparado e o deixa pronto para o próximo uso. */
static int executar(sqlite3_stmt *comando)
{
    int resultado = sqlite3_step(comando);
    sqlite3_reset(comando);
    sqlite3_clear_bindings(comando);
    return resultado;
}

/* Cria as pastas do caminho do banco, como Path.mkdir(parents=True). */
static int criar_pastas(const char *arquivo)
{
    char caminho[4096];
    int tamanho = snprintf(caminho, sizeof caminho, "%s", arquivo);
    if (tamanho < 0 || (size_t)tamanho >= sizeof caminho) {
        return -1;
    }
    char *barra = strrchr(caminho, '/');
    if (barra == NULL || barra == caminho) {
        return 0;
    }
    *barra = '\0';
    for (char *p = caminho + 1; *p != '\0'; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(caminho, 0755) != 0 && errno != EEXIST) {
                return -1;
            }
            *p = '/';
        }
    }
    if (mkdir(caminho, 0755) != 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
}

static void fechar_banco(Banco *banco)
{
    sqlite3_finalize(banco->inserir_login);
    sqlite3_finalize(banco->inserir_canal);
    sqlite3_finalize(banco->listar_canais);
    sqlite3_finalize(banco->inserir_publicacao);
    sqlite3_finalize(banco->inserir_troca);
    /* Fechamos o banco para liberar os recursos usados pelo servidor. */
    sqlite3_close(banco->conexao);
    memset(banco, 0, sizeof *banco);
}

static int abrir_banco(Banco *banco, const char *arquivo)
{
    memset(banco, 0, sizeof *banco);
    /* Criamos a pasta do banco se ainda não existir. */
    if (criar_pastas(arquivo) != 0) {
        printf("Não foi possível criar a pasta do banco %s: %s\n", arquivo, strerror(errno));
        return -1;
    }
    if (sqlite3_open(arquivo, &banco->conexao) != SQLITE_OK) {
        printf("Não foi possível abrir o banco %s: %s\n", arquivo,
               banco->conexao ? sqlite3_errmsg(banco->conexao) : "sem memória");
        sqlite3_close(banco->conexao);
        banco->conexao = NULL;
        return -1;
    }
    /* As tabelas existentes são preservadas ao reiniciar o servidor.
     * Guardamos o evento publicado e os pedidos/respostas completos em binário;
     * assim dá para recuperar os dados sem depender do que apareceu no terminal.
     * Os esquemas são os mesmos do Python. */
    const char *tabelas =
        "CREATE TABLE IF NOT EXISTS logins ("
        "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "    username TEXT NOT NULL,"
        "    timestamp INTEGER NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS canais ("
        "    nome TEXT PRIMARY KEY,"
        "    criado_por TEXT NOT NULL,"
        "    timestamp INTEGER NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS publicacoes ("
        "    id TEXT PRIMARY KEY,"
        "    username TEXT NOT NULL,"
        "    canal TEXT NOT NULL,"
        "    mensagem TEXT NOT NULL,"
        "    timestamp_pedido INTEGER NOT NULL,"
        "    timestamp_publicacao INTEGER NOT NULL,"
        "    evento BLOB NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS trocas ("
        "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "    pedido BLOB NOT NULL,"
        "    resposta BLOB NOT NULL,"
        "    timestamp_recebimento INTEGER NOT NULL"
        ");";
    char *erro = NULL;
    if (sqlite3_exec(banco->conexao, tabelas, NULL, NULL, &erro) != SQLITE_OK) {
        printf("Erro ao criar as tabelas: %s\n", erro ? erro : "desconhecido");
        sqlite3_free(erro);
        fechar_banco(banco);
        return -1;
    }

    struct {
        sqlite3_stmt **destino;
        const char *sql;
    } comandos[] = {
        {&banco->inserir_login, "INSERT INTO logins (username, timestamp) VALUES (?, ?)"},
        {&banco->inserir_canal,
         "INSERT INTO canais (nome, criado_por, timestamp) VALUES (?, ?, ?)"},
        {&banco->listar_canais, "SELECT nome FROM canais ORDER BY nome"},
        {&banco->inserir_publicacao,
         "INSERT INTO publicacoes (id, username, canal, mensagem, timestamp_pedido,"
         " timestamp_publicacao, evento) VALUES (?, ?, ?, ?, ?, ?, ?)"},
        {&banco->inserir_troca,
         "INSERT INTO trocas (pedido, resposta, timestamp_recebimento) VALUES (?, ?, ?)"},
    };
    for (size_t i = 0; i < sizeof comandos / sizeof comandos[0]; i++) {
        if (sqlite3_prepare_v2(banco->conexao, comandos[i].sql, -1, comandos[i].destino, NULL) !=
            SQLITE_OK) {
            printf("Erro ao preparar comando: %s\n", sqlite3_errmsg(banco->conexao));
            fechar_banco(banco);
            return -1;
        }
    }
    return 0;
}

/* Liga uma string MessagePack ao parâmetro do comando (o texto não tem '\0'). */
static int ligar_texto(sqlite3_stmt *comando, int indice, const msgpack_object *texto)
{
    return sqlite3_bind_text(comando, indice, texto->via.str.size ? texto->via.str.ptr : "",
                             (int)texto->via.str.size, SQLITE_TRANSIENT);
}

static int empacotar_resposta(const Resposta *resposta, int64_t instante, msgpack_sbuffer *buffer)
{
    msgpack_packer pk;
    int erro = 0;
    msgpack_packer_init(&pk, buffer, msgpack_sbuffer_write);
    erro |= msgpack_pack_map(&pk, 5);
    erro |= empacotar_texto(&pk, "operation", 9);
    erro |= empacotar_texto(&pk, resposta->operacao, resposta->tamanho_operacao);
    erro |= empacotar_texto(&pk, "timestamp", 9);
    erro |= msgpack_pack_int64(&pk, instante);
    erro |= empacotar_clock(&pk);
    erro |= empacotar_texto(&pk, "status", 6);
    erro |= empacotar_texto(&pk, resposta->status, strlen(resposta->status));
    erro |= empacotar_texto(&pk, "payload", 7);
    switch (resposta->tipo) {
    case DADOS_VAZIO:
        erro |= msgpack_pack_map(&pk, 0);
        break;
    case DADOS_USUARIO:
    case DADOS_CANAL:
        erro |= msgpack_pack_map(&pk, 1);
        erro |= resposta->tipo == DADOS_USUARIO ? empacotar_texto(&pk, "username", 8)
                                                : empacotar_texto(&pk, "channel", 7);
        erro |= empacotar_texto(&pk, resposta->texto, resposta->tamanho_texto);
        break;
    case DADOS_PUBLICACAO:
        erro |= msgpack_pack_map(&pk, 2);
        erro |= empacotar_texto(&pk, "channel", 7);
        erro |= empacotar_texto(&pk, resposta->texto, resposta->tamanho_texto);
        erro |= empacotar_texto(&pk, "id", 2);
        erro |= empacotar_texto(&pk, resposta->id, strlen(resposta->id));
        break;
    case DADOS_CANAIS:
        erro |= msgpack_pack_map(&pk, 1);
        erro |= empacotar_texto(&pk, "channels", 8);
        erro |= msgpack_pack_array(&pk, resposta->quantidade_canais);
        for (size_t i = 0; i < resposta->quantidade_canais; i++) {
            erro |= empacotar_texto(&pk, resposta->canais[i].texto, resposta->canais[i].tamanho);
        }
        break;
    }
    return erro ? -1 : 0;
}

static void tratar_login(const msgpack_object *usuario, int64_t instante, Banco *banco,
                         Resposta *resposta)
{
    /* Salvamos cada login recebido, inclusive nomes repetidos. */
    sqlite3_stmt *comando = banco->inserir_login;
    if (ligar_texto(comando, 1, usuario) != SQLITE_OK ||
        sqlite3_bind_int64(comando, 2, instante) != SQLITE_OK ||
        executar(comando) != SQLITE_DONE) {
        printf("Erro no banco: %s\n", sqlite3_errmsg(banco->conexao));
        fflush(stdout);
        sqlite3_reset(comando);
        sqlite3_clear_bindings(comando);
        definir_erro(resposta, "internal_error");
        return;
    }
    resposta->tipo = DADOS_USUARIO;
    resposta->texto = usuario->via.str.ptr;
    resposta->tamanho_texto = usuario->via.str.size;
}

static void tratar_criacao(const msgpack_object *usuario, const msgpack_object *dados,
                           int64_t instante, Banco *banco, Resposta *resposta)
{
    const msgpack_object *canal = buscar(dados, "channel");
    if (canal == NULL || canal->type != MSGPACK_OBJECT_STR) {
        definir_erro(resposta, "invalid_request");
        return;
    }
    if (!nome_valido(canal)) {
        definir_erro(resposta, "invalid_name");
        return;
    }
    sqlite3_stmt *comando = banco->inserir_canal;
    if (ligar_texto(comando, 1, canal) != SQLITE_OK || ligar_texto(comando, 2, usuario) != SQLITE_OK ||
        sqlite3_bind_int64(comando, 3, instante) != SQLITE_OK) {
        sqlite3_reset(comando);
        sqlite3_clear_bindings(comando);
        definir_erro(resposta, "internal_error");
        return;
    }
    int resultado = sqlite3_step(comando);
    sqlite3_reset(comando);
    sqlite3_clear_bindings(comando);
    /* A chave primária do canal impede cadastrar o mesmo nome duas vezes. */
    if (resultado == SQLITE_CONSTRAINT) {
        definir_erro(resposta, "channel_exists");
        return;
    }
    if (resultado != SQLITE_DONE) {
        printf("Erro no banco: %s\n", sqlite3_errmsg(banco->conexao));
        fflush(stdout);
        definir_erro(resposta, "internal_error");
        return;
    }
    resposta->tipo = DADOS_CANAL;
    resposta->texto = canal->via.str.ptr;
    resposta->tamanho_texto = canal->via.str.size;
}

static void tratar_listagem(Banco *banco, Resposta *resposta)
{
    /* Buscamos os canais em ordem alfabética e devolvemos só os nomes. */
    sqlite3_stmt *comando = banco->listar_canais;
    size_t capacidade = 0;
    int resultado;
    resposta->tipo = DADOS_CANAIS;
    while ((resultado = sqlite3_step(comando)) == SQLITE_ROW) {
        const unsigned char *nome = sqlite3_column_text(comando, 0);
        int tamanho = sqlite3_column_bytes(comando, 0);
        if (nome == NULL || tamanho < 0) {
            resultado = SQLITE_NOMEM;
            break;
        }
        if (resposta->quantidade_canais == capacidade) {
            size_t nova = capacidade ? capacidade * 2 : 16;
            Texto *maior = realloc(resposta->canais, nova * sizeof *maior);
            if (maior == NULL) {
                resultado = SQLITE_NOMEM;
                break;
            }
            resposta->canais = maior;
            capacidade = nova;
        }
        char *copia = malloc((size_t)tamanho + 1);
        if (copia == NULL) {
            resultado = SQLITE_NOMEM;
            break;
        }
        memcpy(copia, nome, (size_t)tamanho + 1);
        resposta->canais[resposta->quantidade_canais].texto = copia;
        resposta->canais[resposta->quantidade_canais].tamanho = (size_t)tamanho;
        resposta->quantidade_canais++;
    }
    sqlite3_reset(comando);
    if (resultado != SQLITE_DONE) {
        printf("Erro no banco: %s\n", sqlite3_errmsg(banco->conexao));
        fflush(stdout);
        definir_erro(resposta, "internal_error");
    }
}

static void tratar_publicacao(const msgpack_object *usuario, const msgpack_object *dados,
                              int64_t instante, Banco *banco, void *publicador,
                              Resposta *resposta)
{
    const msgpack_object *canal = buscar(dados, "channel");
    const msgpack_object *texto = buscar(dados, "message");
    if (canal == NULL || canal->type != MSGPACK_OBJECT_STR || texto == NULL ||
        texto->type != MSGPACK_OBJECT_STR) {
        definir_erro(resposta, "invalid_request");
        return;
    }
    if (!nome_valido(canal)) {
        definir_erro(resposta, "invalid_name");
        return;
    }
    if (!mensagem_valida(texto->via.str.ptr, texto->via.str.size)) {
        definir_erro(resposta, "invalid_message");
        return;
    }

    /* O canal pode ter sido criado em outro servidor. Sem replicação,
     * não usamos a lista local para recusar um canal escolhido pelo bot. */
    uuid_t bruto;
    uuid_generate_random(bruto);
    uuid_unparse_lower(bruto, resposta->id);
    int64_t instante_evento = timestamp_ms();

    msgpack_sbuffer evento;
    msgpack_sbuffer topico;
    msgpack_sbuffer_init(&evento);
    msgpack_sbuffer_init(&topico);
    msgpack_packer pk;
    int erro = 0;
    msgpack_packer_init(&pk, &evento, msgpack_sbuffer_write);
    erro |= msgpack_pack_map(&pk, 4);
    erro |= empacotar_texto(&pk, "operation", 9);
    erro |= empacotar_texto(&pk, "publication", 11);
    erro |= empacotar_texto(&pk, "timestamp", 9);
    erro |= msgpack_pack_int64(&pk, instante_evento);
    erro |= empacotar_clock(&pk);
    erro |= empacotar_texto(&pk, "payload", 7);
    erro |= msgpack_pack_map(&pk, 5);
    erro |= empacotar_texto(&pk, "id", 2);
    erro |= empacotar_texto(&pk, resposta->id, strlen(resposta->id));
    erro |= empacotar_texto(&pk, "username", 8);
    erro |= empacotar_texto(&pk, usuario->via.str.ptr, usuario->via.str.size);
    erro |= empacotar_texto(&pk, "channel", 7);
    erro |= empacotar_texto(&pk, canal->via.str.ptr, canal->via.str.size);
    erro |= empacotar_texto(&pk, "message", 7);
    erro |= empacotar_texto(&pk, texto->via.str.size ? texto->via.str.ptr : "", texto->via.str.size);
    erro |= empacotar_texto(&pk, "request_timestamp", 17);
    erro |= msgpack_pack_int64(&pk, instante);
    /* O tópico também é MessagePack: o prefixo de tamanho distingue canal1 de canal10. */
    msgpack_packer_init(&pk, &topico, msgpack_sbuffer_write);
    erro |= empacotar_texto(&pk, canal->via.str.ptr, canal->via.str.size);
    if (erro) {
        definir_erro(resposta, "internal_error");
        goto fim;
    }

    /* Primeiro salvamos em disco; só depois tentamos publicar. */
    sqlite3_stmt *comando = banco->inserir_publicacao;
    if (sqlite3_bind_text(comando, 1, resposta->id, -1, SQLITE_TRANSIENT) != SQLITE_OK ||
        ligar_texto(comando, 2, usuario) != SQLITE_OK || ligar_texto(comando, 3, canal) != SQLITE_OK ||
        ligar_texto(comando, 4, texto) != SQLITE_OK ||
        sqlite3_bind_int64(comando, 5, instante) != SQLITE_OK ||
        sqlite3_bind_int64(comando, 6, instante_evento) != SQLITE_OK ||
        sqlite3_bind_blob(comando, 7, evento.data, (int)evento.size, SQLITE_TRANSIENT) != SQLITE_OK ||
        executar(comando) != SQLITE_DONE) {
        printf("Erro no banco: %s\n", sqlite3_errmsg(banco->conexao));
        fflush(stdout);
        sqlite3_reset(comando);
        sqlite3_clear_bindings(comando);
        definir_erro(resposta, "internal_error");
        goto fim;
    }

    /* Dois frames binários: nome do tópico em MessagePack e evento. */
    if (zmq_send(publicador, topico.data, topico.size, ZMQ_SNDMORE) < 0 ||
        zmq_send(publicador, evento.data, evento.size, 0) < 0) {
        printf("Erro ao publicar: %s\n", zmq_strerror(zmq_errno()));
        fflush(stdout);
        definir_erro(resposta, "internal_error");
        goto fim;
    }
    mostrar_empacotada("PUBLICANDO", evento.data, evento.size);
    resposta->tipo = DADOS_PUBLICACAO;
    resposta->texto = canal->via.str.ptr;
    resposta->tamanho_texto = canal->via.str.size;

fim:
    msgpack_sbuffer_destroy(&evento);
    msgpack_sbuffer_destroy(&topico);
}

static void tratar_pedido(const msgpack_object *pedido, Banco *banco, void *publicador,
                          Resposta *resposta)
{
    /* Antes de executar a operação, conferimos os campos e seus tipos. */
    const msgpack_object *operacao = buscar(pedido, "operation");
    const msgpack_object *instante = buscar(pedido, "timestamp");
    const msgpack_object *dados = buscar(pedido, "payload");
    if (operacao == NULL || instante == NULL || dados == NULL ||
        operacao->type != MSGPACK_OBJECT_STR ||
        instante->type != MSGPACK_OBJECT_POSITIVE_INTEGER || instante->via.u64 > INT64_MAX ||
        dados->type != MSGPACK_OBJECT_MAP) {
        definir_erro(resposta, "invalid_request");
        return;
    }
    int64_t instante_pedido = (int64_t)instante->via.u64;

    bool com_usuario = texto_igual(operacao, "login") || texto_igual(operacao, "create_channel") ||
                       texto_igual(operacao, "publish");
    const msgpack_object *usuario = NULL;
    if (com_usuario) {
        usuario = buscar(dados, "username");
        if (usuario == NULL || usuario->type != MSGPACK_OBJECT_STR) {
            definir_erro(resposta, "invalid_request");
            return;
        }
        if (!nome_valido(usuario)) {
            definir_erro(resposta, "invalid_name");
            return;
        }
    }

    if (texto_igual(operacao, "login")) {
        tratar_login(usuario, instante_pedido, banco, resposta);
    } else if (texto_igual(operacao, "create_channel")) {
        tratar_criacao(usuario, dados, instante_pedido, banco, resposta);
    } else if (texto_igual(operacao, "publish")) {
        tratar_publicacao(usuario, dados, instante_pedido, banco, publicador, resposta);
    } else if (texto_igual(operacao, "list_channels")) {
        tratar_listagem(banco, resposta);
    } else {
        definir_erro(resposta, "unknown_operation");
    }
}

/* Registra o pedido e a resposta completos na tabela de trocas. */
static int registrar_troca(Banco *banco, const void *pedido, size_t tamanho_pedido,
                           const msgpack_sbuffer *resposta, int64_t recebido_em)
{
    sqlite3_stmt *comando = banco->inserir_troca;
    /* Um pedido vazio ainda precisa gravar um BLOB, não NULL (NOT NULL). */
    if (sqlite3_bind_blob(comando, 1, tamanho_pedido ? pedido : "", (int)tamanho_pedido,
                          SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_blob(comando, 2, resposta->data, (int)resposta->size, SQLITE_STATIC) !=
            SQLITE_OK ||
        sqlite3_bind_int64(comando, 3, recebido_em) != SQLITE_OK) {
        sqlite3_reset(comando);
        sqlite3_clear_bindings(comando);
        printf("Falha ao registrar troca: %s\n", sqlite3_errmsg(banco->conexao));
        fflush(stdout);
        return -1;
    }
    if (executar(comando) != SQLITE_DONE) {
        printf("Falha ao registrar troca: %s\n", sqlite3_errmsg(banco->conexao));
        fflush(stdout);
        return -1;
    }
    return 0;
}

/* Último recurso sem alocação dinâmica, caso o buffer MessagePack falhe. */
static size_t montar_reserva(char *buffer)
{
    static const unsigned char inicio[] = "\x85\xa9" "operation" "\xa0\xa9" "timestamp" "\xd3";
    static const unsigned char meio[] = "\xa5" "clock" "\xd3";
    static const unsigned char fim[] = "\xa6" "status" "\xae" "internal_error" "\xa7" "payload" "\x80";
    size_t posicao = 0;
    memcpy(buffer + posicao, inicio, sizeof inicio - 1); posicao += sizeof inicio - 1;
    uint64_t valores[2] = {(uint64_t)timestamp_ms(), (uint64_t)relogio_enviar()};
    for (int campo = 0; campo < 2; campo++) {
        if (campo == 1) {
            memcpy(buffer + posicao, meio, sizeof meio - 1); posicao += sizeof meio - 1;
        }
        for (int byte = 7; byte >= 0; byte--)
            buffer[posicao++] = (char)(valores[campo] >> (byte * 8));
    }
    memcpy(buffer + posicao, fim, sizeof fim - 1); posicao += sizeof fim - 1;
    return posicao;
}

static void atender(void *socket, void *publicador, Banco *banco, zmq_msg_t *mensagem)
{
    const char *dados = zmq_msg_data(mensagem);
    size_t tamanho = zmq_msg_size(mensagem);
    int64_t recebido_em = timestamp_ms();
    Resposta resposta = {.operacao = "", .status = "ok", .tipo = DADOS_VAZIO};
    msgpack_unpacked pedido;
    msgpack_unpacked_init(&pedido);

    if (decodificar(dados, tamanho, &pedido) != 0) {
        mostrar_bytes_invalidos(dados, tamanho);
        /* Até um pacote inválido recebe resposta para manter o ciclo do REP. */
        definir_erro(&resposta, "invalid_message");
    } else {
        /* Separamos visualmente o envio do bot e o recebimento aqui. */
        pausar(pausa_visual);
        mostrar_mensagem("RECEBENDO", &pedido.data);
        pausar(pausa_visual);
        if (pedido.data.type != MSGPACK_OBJECT_MAP || relogio_receber(&pedido.data) != 0) {
            definir_erro(&resposta, "invalid_request");
        } else {
            /* A resposta repete a operação recebida, se ela for uma string. */
            const msgpack_object *operacao = buscar(&pedido.data, "operation");
            if (operacao != NULL && operacao->type == MSGPACK_OBJECT_STR) {
                resposta.operacao = operacao->via.str.size ? operacao->via.str.ptr : "";
                resposta.tamanho_operacao = operacao->via.str.size;
            }
            tratar_pedido(&pedido.data, banco, publicador, &resposta);
        }
    }

    /* REP precisa responder antes de receber outro pedido.
     * Calculamos o horário depois das pausas, perto do envio. */
    msgpack_sbuffer saida;
    msgpack_sbuffer_init(&saida);
    bool montada = empacotar_resposta(&resposta, timestamp_ms(), &saida) == 0;
    /* Também registramos a requisição e a resposta completas. */
    if (montada && registrar_troca(banco, dados, tamanho, &saida, recebido_em) != 0) {
        definir_erro(&resposta, "internal_error");
        msgpack_sbuffer_clear(&saida);
        montada = empacotar_resposta(&resposta, timestamp_ms(), &saida) == 0;
    }

    char reserva[128];
    const char *envio;
    size_t tamanho_envio;
    if (montada) {
        envio = saida.data;
        tamanho_envio = saida.size;
    } else {
        tamanho_envio = montar_reserva(reserva);
        envio = reserva;
    }
    mostrar_empacotada("RESPONDENDO", envio, tamanho_envio);
    while (zmq_send(socket, envio, tamanho_envio, 0) < 0 && zmq_errno() == EINTR) {
        /* Um sinal no meio do envio não pode deixar o pedido sem resposta. */
    }

    msgpack_sbuffer_destroy(&saida);
    msgpack_unpacked_destroy(&pedido);
    liberar_canais(&resposta);
}

int main(void)
{
    relogio_iniciar_servidor();
    /* Cada servidor pode receber um nome e um caminho de banco próprios. */
    const char *endereco_broker = ambiente("ENDERECO_BROKER", "tcp://localhost:5556");
    const char *endereco_publicacao = ambiente("ENDERECO_PUBLICACAO", "tcp://localhost:5557");
    /* Cada réplica recebe uma identificação diferente nos registros. */
    const char *identificador = getenv("HOSTNAME");
    char nome_padrao[64];
    if (identificador != NULL && identificador[0] != '\0') {
        snprintf(nome_padrao, sizeof nome_padrao, "servc-%.12s", identificador);
    } else {
        snprintf(nome_padrao, sizeof nome_padrao, "servc");
    }
    snprintf(nome_servidor, sizeof nome_servidor, "%s", ambiente("NOME_SERVIDOR", nome_padrao));
    char banco_padrao[512];
    snprintf(banco_padrao, sizeof banco_padrao, "dados/%s.db", nome_servidor);
    const char *arquivo_banco = ambiente("ARQUIVO_BANCO", banco_padrao);
    /* Pausa visual da demonstração; use PAUSA_VISUAL=0 para desativar. */
    pausa_visual = strtod(ambiente("PAUSA_VISUAL", "0"), NULL);
    if (isnan(pausa_visual) || pausa_visual < 0) {
        pausa_visual = 0;
    } else if (pausa_visual > 1) {
        pausa_visual = 1;
    }

    /* Sem SA_RESTART, o recv bloqueado retorna com EINTR e o laço termina. */
    struct sigaction acao;
    memset(&acao, 0, sizeof acao);
    acao.sa_handler = tratar_sinal;
    sigemptyset(&acao.sa_mask);
    sigaction(SIGINT, &acao, NULL);
    sigaction(SIGTERM, &acao, NULL);

    Banco banco;
    if (abrir_banco(&banco, arquivo_banco) != 0) {
        return 1;
    }

    int codigo = 1;
    void *socket = NULL;
    void *publicador = NULL;
    void *contexto = zmq_ctx_new();
    if (contexto == NULL) {
        printf("Erro ao criar o contexto ZeroMQ: %s\n", zmq_strerror(zmq_errno()));
        goto fim;
    }
    /* REP deve responder ao pedido atual antes de receber outro. */
    socket = zmq_socket(contexto, ZMQ_REP);
    publicador = zmq_socket(contexto, ZMQ_PUB);
    if (socket == NULL || publicador == NULL || zmq_connect(socket, endereco_broker) != 0 ||
        zmq_connect(publicador, endereco_publicacao) != 0) {
        printf("Erro ao conectar: %s\n", zmq_strerror(zmq_errno()));
        goto fim;
    }
    printf("%s iniciado. Banco: %s\n", nome_servidor, arquivo_banco);
    fflush(stdout);

    Referencia referencia = {
        .contexto = contexto, .nome = nome_servidor,
        .endereco = ambiente("ENDERECO_REFERENCIA", "tcp://localhost:5559"),
        .rank = 0, .proxima_tentativa = 0
    };
    int prazo_recebimento = 200;
    zmq_setsockopt(socket, ZMQ_RCVTIMEO, &prazo_recebimento, sizeof prazo_recebimento);
    if (referencia_registrar(&referencia)) referencia_listar(&referencia);
    int recebidas = 0;
    codigo = 0;
    while (!parar) {
        referencia_manter(&referencia);
        /* Aqui aguardamos um pedido, sem gastar CPU consultando em loop. */
        zmq_msg_t mensagem;
        zmq_msg_init(&mensagem);
        if (zmq_msg_recv(&mensagem, socket, 0) < 0) {
            int erro = zmq_errno();
            zmq_msg_close(&mensagem);
            if (erro == EINTR || erro == EAGAIN) {
                continue;
            }
            printf("Erro ao receber: %s\n", zmq_strerror(erro));
            codigo = 1;
            break;
        }
        /* O protocolo usa um frame por pedido; partes extras são descartadas. */
        int mais = zmq_msg_more(&mensagem);
        while (mais) {
            zmq_msg_t extra;
            zmq_msg_init(&extra);
            if (zmq_msg_recv(&extra, socket, 0) < 0 && zmq_errno() != EINTR) {
                zmq_msg_close(&extra);
                break;
            }
            mais = zmq_msg_more(&extra);
            zmq_msg_close(&extra);
        }
        atender(socket, publicador, &banco, &mensagem);
        zmq_msg_close(&mensagem);
        if (++recebidas >= 10) {
            referencia_heartbeat(&referencia);
            recebidas = 0;
        }
    }
    if (parar) {
        printf("Servidor encerrado.\n");
    }

fim:
    fflush(stdout);
    if (publicador != NULL) {
        int zero = 0;
        zmq_setsockopt(publicador, ZMQ_LINGER, &zero, sizeof zero);
        zmq_close(publicador);
    }
    if (socket != NULL) {
        int zero = 0;
        zmq_setsockopt(socket, ZMQ_LINGER, &zero, sizeof zero);
        zmq_close(socket);
    }
    if (contexto != NULL) {
        zmq_ctx_term(contexto);
    }
    fechar_banco(&banco);
    return codigo;
}
