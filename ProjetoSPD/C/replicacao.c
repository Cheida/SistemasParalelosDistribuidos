/* Parte 5: replica logins, canais, publicações e trocas entre os servidores.
 * Segue o mesmo protocolo do python/replicacao.py e do Replicacao.java:
 * cada servidor guarda um log de eventos (replica_eventos), atende pedidos
 * {"desde": seq} no REP da porta 5561 e busca periodicamente os eventos novos
 * dos outros servidores listados pela referência. A replicação é assíncrona e
 * eventualmente consistente: as cópias convergem alguns segundos depois.
 * Uma thread atende o REP e outra captura, sincroniza e envia heartbeat; cada
 * uma tem sua própria conexão SQLite e seus próprios sockets ZeroMQ. */
#define _POSIX_C_SOURCE 200809L
#include "replicacao.h"
#include "protocolo.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sqlite3.h>
#include <zmq.h>

#define LOTE 100
#define MAX_LOTES 10
#define PRAZO_PAR_MS 350
#define PRAZO_REFERENCIA_MS 1000
#define INTERVALO_HEARTBEAT 20.0

typedef struct {
    const char *nome;
    const char *colunas[7];
    int quantidade;
} Tabela;

/* Mesmas colunas, na mesma ordem, do Python: o receptor confere as chaves. */
static const Tabela tabelas[] = {
    {"logins", {"id", "username", "timestamp"}, 3},
    {"canais", {"nome", "criado_por", "timestamp"}, 3},
    {"publicacoes",
     {"id", "username", "canal", "mensagem", "timestamp_pedido", "timestamp_publicacao", "evento"},
     7},
    {"trocas", {"id", "pedido", "resposta", "timestamp_recebimento"}, 4},
};
#define QUANTIDADE_TABELAS (sizeof tabelas / sizeof tabelas[0])

static void *contexto;
static const char *nome;
static const char *arquivo;
static const char *endereco_referencia;
static double intervalo = 1.0;
static void *rep;
static pthread_t ouvinte, trabalhador;
static bool iniciado;
static atomic_bool parar;

static void dormir(double segundos)
{
    struct timespec t = {(time_t)segundos, (long)((segundos - (time_t)segundos) * 1e9)};
    nanosleep(&t, NULL);
}

static void avisar(const char *texto, const char *detalhe)
{
    printf("[%s] RÉPLICA | erro: %s%s%s\n", nome, texto, detalhe ? ": " : "", detalhe ? detalhe : "");
    fflush(stdout);
}

static sqlite3 *conectar(void)
{
    sqlite3 *banco = NULL;
    if (sqlite3_open(arquivo, &banco) != SQLITE_OK) {
        avisar("não foi possível abrir o banco", banco ? sqlite3_errmsg(banco) : NULL);
        sqlite3_close(banco);
        return NULL;
    }
    /* A outra thread e o servidor escrevem no mesmo arquivo. */
    sqlite3_busy_timeout(banco, 5000);
    return banco;
}

static int executar(sqlite3 *banco, const char *sql)
{
    char *erro = NULL;
    if (sqlite3_exec(banco, sql, NULL, NULL, &erro) != SQLITE_OK) {
        avisar(sql, erro);
        sqlite3_free(erro);
        return -1;
    }
    return 0;
}

static const Tabela *achar_tabela(const msgpack_object *tipo)
{
    for (size_t i = 0; i < QUANTIDADE_TABELAS; i++)
        if (texto_igual(tipo, tabelas[i].nome)) return &tabelas[i];
    return NULL;
}

/* Lista as colunas a partir de `inicio` ("a, b, c" ou "a=? AND b=?"), para
 * montar o SQL. Os nomes vêm da tabela fixa acima, nunca de dados recebidos. */
static void listar_colunas(const Tabela *t, int inicio, const char *separador, const char *sufixo,
                           char *saida, size_t tamanho)
{
    size_t usado = 0;
    saida[0] = '\0';
    for (int i = inicio; i < t->quantidade && usado < tamanho; i++)
        usado += (size_t)snprintf(saida + usado, tamanho - usado, "%s%s%s",
                                  i > inicio ? separador : "", t->colunas[i], sufixo);
}

static void listar_marcadores(int quantidade, char *saida, size_t tamanho)
{
    size_t usado = 0;
    saida[0] = '\0';
    for (int i = 0; i < quantidade && usado < tamanho; i++)
        usado += (size_t)snprintf(saida + usado, tamanho - usado, "%s?", i ? ", " : "");
}

/* Empacota o valor de uma coluna com o tipo que o Python produziria:
 * INTEGER vira int, TEXT vira str e BLOB vira bin. */
static int empacotar_coluna(msgpack_packer *pk, sqlite3_stmt *c, int i)
{
    switch (sqlite3_column_type(c, i)) {
    case SQLITE_INTEGER:
        return msgpack_pack_int64(pk, sqlite3_column_int64(c, i));
    case SQLITE_FLOAT:
        return msgpack_pack_double(pk, sqlite3_column_double(c, i));
    case SQLITE_TEXT: {
        const char *texto = (const char *)sqlite3_column_text(c, i);
        return empacotar_texto(pk, texto ? texto : "", (size_t)sqlite3_column_bytes(c, i));
    }
    case SQLITE_BLOB: {
        int tamanho = sqlite3_column_bytes(c, i);
        const void *dados = sqlite3_column_blob(c, i);
        int erro = msgpack_pack_bin(pk, (size_t)tamanho);
        if (tamanho > 0) erro |= msgpack_pack_bin_body(pk, dados, (size_t)tamanho);
        return erro;
    }
    default:
        return msgpack_pack_nil(pk);
    }
}

/* Liga um valor MessagePack recebido a um parâmetro do comando SQL. */
static int ligar_objeto(sqlite3_stmt *c, int indice, const msgpack_object *o)
{
    switch (o->type) {
    case MSGPACK_OBJECT_POSITIVE_INTEGER:
        if (o->via.u64 > INT64_MAX) return SQLITE_RANGE;
        return sqlite3_bind_int64(c, indice, (sqlite3_int64)o->via.u64);
    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
        return sqlite3_bind_int64(c, indice, o->via.i64);
    case MSGPACK_OBJECT_FLOAT32:
    case MSGPACK_OBJECT_FLOAT64:
        return sqlite3_bind_double(c, indice, o->via.f64);
    case MSGPACK_OBJECT_STR:
        return sqlite3_bind_text(c, indice, o->via.str.size ? o->via.str.ptr : "",
                                 (int)o->via.str.size, SQLITE_TRANSIENT);
    case MSGPACK_OBJECT_BIN:
        return sqlite3_bind_blob(c, indice, o->via.bin.size ? o->via.bin.ptr : "",
                                 (int)o->via.bin.size, SQLITE_TRANSIENT);
    case MSGPACK_OBJECT_NIL:
        return sqlite3_bind_null(c, indice);
    default:
        return SQLITE_MISMATCH;
    }
}

static int ligar_texto(sqlite3_stmt *c, int indice, const char *texto)
{
    return sqlite3_bind_text(c, indice, texto, -1, SQLITE_TRANSIENT);
}

/* Prepara, liga textos (NULL encerra a lista) e executa um comando sem resultado.
 * Devolve o número de linhas alteradas ou -1 em erro. */
static int alterar(sqlite3 *banco, const char *sql, const char *a, const char *b, const char *c)
{
    sqlite3_stmt *comando = NULL;
    if (sqlite3_prepare_v2(banco, sql, -1, &comando, NULL) != SQLITE_OK) {
        avisar(sqlite3_errmsg(banco), sql);
        return -1;
    }
    const char *valores[] = {a, b, c};
    for (int i = 0; i < 3 && valores[i] != NULL; i++) ligar_texto(comando, i + 1, valores[i]);
    int resultado = sqlite3_step(comando) == SQLITE_DONE ? sqlite3_changes(banco) : -1;
    if (resultado < 0) avisar(sqlite3_errmsg(banco), sql);
    sqlite3_finalize(comando);
    return resultado;
}

/* Grava os dados empacotados no log de eventos. */
static int gravar_evento(sqlite3 *banco, const char *origem, const char *tipo, const char *chave,
                         const void *dados, size_t tamanho)
{
    sqlite3_stmt *c = NULL;
    const char *sql =
        "INSERT OR IGNORE INTO replica_eventos (origem, tipo, chave, dados) VALUES (?, ?, ?, ?)";
    if (sqlite3_prepare_v2(banco, sql, -1, &c, NULL) != SQLITE_OK) return -1;
    ligar_texto(c, 1, origem);
    ligar_texto(c, 2, tipo);
    ligar_texto(c, 3, chave);
    sqlite3_bind_blob(c, 4, tamanho ? dados : "", (int)tamanho, SQLITE_TRANSIENT);
    int resultado = sqlite3_step(c) == SQLITE_DONE ? sqlite3_changes(banco) : -1;
    sqlite3_finalize(c);
    return resultado;
}

/* Empacota a linha atual de `c` como mapa {coluna: valor}. */
static int empacotar_linha(const Tabela *t, sqlite3_stmt *c, msgpack_sbuffer *saida)
{
    msgpack_packer pk;
    msgpack_packer_init(&pk, saida, msgpack_sbuffer_write);
    int erro = msgpack_pack_map(&pk, (size_t)t->quantidade);
    for (int i = 0; i < t->quantidade; i++) {
        erro |= empacotar_texto(&pk, t->colunas[i], strlen(t->colunas[i]));
        erro |= empacotar_coluna(&pk, c, i);
    }
    return erro;
}

static int preparar(sqlite3 *banco)
{
    /* Os esquemas são iguais aos do Python e do Java. */
    const char *esquema =
        "CREATE TABLE IF NOT EXISTS replica_eventos ("
        "    seq INTEGER PRIMARY KEY AUTOINCREMENT,"
        "    origem TEXT NOT NULL,"
        "    tipo TEXT NOT NULL,"
        "    chave TEXT NOT NULL,"
        "    dados BLOB NOT NULL,"
        "    UNIQUE (origem, tipo, chave)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_replica_evento_chave ON replica_eventos (tipo, chave);"
        "CREATE TABLE IF NOT EXISTS replica_capturados ("
        "    tipo TEXT NOT NULL,"
        "    chave_local TEXT NOT NULL,"
        "    PRIMARY KEY (tipo, chave_local)"
        ");"
        "CREATE TABLE IF NOT EXISTS replica_importados ("
        "    tipo TEXT NOT NULL,"
        "    chave_local TEXT NOT NULL,"
        "    PRIMARY KEY (tipo, chave_local)"
        ");"
        "CREATE TABLE IF NOT EXISTS replica_cursores ("
        "    servidor TEXT PRIMARY KEY,"
        "    seq INTEGER NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS replica_vencedor_canal ("
        "    nome TEXT PRIMARY KEY,"
        "    timestamp INTEGER NOT NULL,"
        "    origem TEXT NOT NULL"
        ");";
    if (executar(banco, esquema) != 0) return -1;

    /* Migração igual à do Python: linhas antigas que já têm um evento com os
     * mesmos bytes passam a contar como capturadas. */
    if (executar(banco, "BEGIN IMMEDIATE") != 0) return -1;
    int erro = 0;
    for (size_t i = 0; i < QUANTIDADE_TABELAS && !erro; i++) {
        const Tabela *t = &tabelas[i];
        char colunas[256], sql[512];
        listar_colunas(t, 0, ", ", "", colunas, sizeof colunas);
        snprintf(sql, sizeof sql, "SELECT %s, CAST(%s AS TEXT) FROM %s", colunas, t->colunas[0],
                 t->nome);
        sqlite3_stmt *c = NULL;
        if (sqlite3_prepare_v2(banco, sql, -1, &c, NULL) != SQLITE_OK) {
            erro = 1;
            break;
        }
        while (!erro && sqlite3_step(c) == SQLITE_ROW) {
            const char *chave = (const char *)sqlite3_column_text(c, t->quantidade);
            msgpack_sbuffer dados;
            msgpack_sbuffer_init(&dados);
            if (chave == NULL || empacotar_linha(t, c, &dados) != 0) {
                erro = 1;
            } else {
                sqlite3_stmt *existe = NULL;
                if (sqlite3_prepare_v2(banco,
                                       "SELECT 1 FROM replica_eventos WHERE tipo=? AND chave=? AND "
                                       "dados=? LIMIT 1",
                                       -1, &existe, NULL) != SQLITE_OK) {
                    erro = 1;
                } else {
                    ligar_texto(existe, 1, t->nome);
                    ligar_texto(existe, 2, chave);
                    sqlite3_bind_blob(existe, 3, dados.data, (int)dados.size, SQLITE_TRANSIENT);
                    if (sqlite3_step(existe) == SQLITE_ROW &&
                        alterar(banco, "INSERT OR IGNORE INTO replica_capturados VALUES (?, ?)",
                                t->nome, chave, NULL) < 0)
                        erro = 1;
                    sqlite3_finalize(existe);
                }
            }
            msgpack_sbuffer_destroy(&dados);
        }
        sqlite3_finalize(c);
    }
    executar(banco, erro ? "ROLLBACK" : "COMMIT");
    return erro ? -1 : 0;
}

/* Registra como eventos as linhas criadas por este servidor. Linhas que
 * vieram de outro servidor (replica_importados) não viram eventos novos. */
static int capturar_tabela(sqlite3 *banco, const Tabela *t)
{
    const char *k = t->colunas[0];
    char colunas[256], sql[1024];
    listar_colunas(t, 0, ", ", "", colunas, sizeof colunas);
    snprintf(sql, sizeof sql,
             "SELECT %s, CAST(t.%s AS TEXT) FROM %s AS t "
             "WHERE NOT EXISTS (SELECT 1 FROM replica_importados i WHERE i.tipo=?1 "
             "AND i.chave_local=CAST(t.%s AS TEXT)) "
             "AND NOT EXISTS (SELECT 1 FROM replica_capturados c WHERE c.tipo=?1 "
             "AND c.chave_local=CAST(t.%s AS TEXT)) "
             "AND NOT EXISTS (SELECT 1 FROM replica_eventos e WHERE e.origem=?2 AND e.tipo=?1 "
             "AND e.chave=CAST(t.%s AS TEXT)) LIMIT %d",
             colunas, k, t->nome, k, k, k, LOTE);

    if (executar(banco, "BEGIN IMMEDIATE") != 0) return -1;
    sqlite3_stmt *c = NULL;
    int erro = sqlite3_prepare_v2(banco, sql, -1, &c, NULL) != SQLITE_OK;
    if (!erro) {
        ligar_texto(c, 1, t->nome);
        ligar_texto(c, 2, nome);
    }
    int passo;
    while (!erro && (passo = sqlite3_step(c)) == SQLITE_ROW) {
        const char *chave = (const char *)sqlite3_column_text(c, t->quantidade);
        msgpack_sbuffer dados;
        msgpack_sbuffer_init(&dados);
        if (chave == NULL || empacotar_linha(t, c, &dados) != 0 ||
            gravar_evento(banco, nome, t->nome, chave, dados.data, dados.size) < 0 ||
            alterar(banco, "INSERT OR IGNORE INTO replica_capturados VALUES (?, ?)", t->nome, chave,
                    NULL) < 0) {
            erro = 1;
        } else if (strcmp(t->nome, "canais") == 0) {
            /* O canal criado aqui é o vencedor até chegar um mais antigo. */
            sqlite3_stmt *v = NULL;
            if (sqlite3_prepare_v2(banco,
                                   "INSERT OR IGNORE INTO replica_vencedor_canal VALUES (?, ?, ?)",
                                   -1, &v, NULL) != SQLITE_OK) {
                erro = 1;
            } else {
                ligar_texto(v, 1, chave);
                sqlite3_bind_int64(v, 2, sqlite3_column_int64(c, 2));
                ligar_texto(v, 3, nome);
                if (sqlite3_step(v) != SQLITE_DONE) erro = 1;
                sqlite3_finalize(v);
            }
        }
        msgpack_sbuffer_destroy(&dados);
    }
    if (erro) avisar("falha ao capturar", sqlite3_errmsg(banco));
    sqlite3_finalize(c);
    executar(banco, erro ? "ROLLBACK" : "COMMIT");
    return erro ? -1 : 0;
}

/* Texto da chave como o str() do Python: inteiros em decimal, textos iguais. */
static bool chave_do_valor(const msgpack_object *o, char *saida, size_t tamanho)
{
    if (o == NULL) return false;
    if (o->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        snprintf(saida, tamanho, "%" PRIu64, o->via.u64);
        return true;
    }
    if (o->type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
        snprintf(saida, tamanho, "%" PRId64, o->via.i64);
        return true;
    }
    if (o->type == MSGPACK_OBJECT_STR && o->via.str.size < tamanho &&
        memchr(o->via.str.ptr, '\0', o->via.str.size) == NULL) {
        memcpy(saida, o->via.str.ptr, o->via.str.size);
        saida[o->via.str.size] = '\0';
        return true;
    }
    return false;
}

/* Copia uma string MessagePack sem '\0' interno para um buffer terminado em '\0'. */
static bool copiar_texto(const msgpack_object *o, char *saida, size_t tamanho)
{
    if (o == NULL || o->type != MSGPACK_OBJECT_STR || o->via.str.size >= tamanho ||
        memchr(o->via.str.ptr, '\0', o->via.str.size) != NULL)
        return false;
    memcpy(saida, o->via.str.ptr, o->via.str.size);
    saida[o->via.str.size] = '\0';
    return true;
}

/* As chaves de `dados` devem ser exatamente as colunas da tabela. */
static bool colunas_conferem(const Tabela *t, const msgpack_object *dados)
{
    if (dados->type != MSGPACK_OBJECT_MAP) return false;
    for (int i = 0; i < t->quantidade; i++)
        if (buscar(dados, t->colunas[i]) == NULL) return false;
    for (uint32_t i = 0; i < dados->via.map.size; i++) {
        bool conhecida = false;
        for (int j = 0; j < t->quantidade; j++)
            if (texto_igual(&dados->via.map.ptr[i].key, t->colunas[j])) conhecida = true;
        if (!conhecida) return false;
    }
    return true;
}

/* Executa `sql` ligando as colunas de `dados` a partir de `inicio`. */
static int alterar_com_dados(sqlite3 *banco, const char *sql, const Tabela *t, int inicio,
                             const msgpack_object *dados)
{
    sqlite3_stmt *c = NULL;
    if (sqlite3_prepare_v2(banco, sql, -1, &c, NULL) != SQLITE_OK) return -1;
    int resultado = 0;
    for (int i = inicio; i < t->quantidade; i++)
        if (ligar_objeto(c, i - inicio + 1, buscar(dados, t->colunas[i])) != SQLITE_OK) resultado = -1;
    if (resultado == 0) resultado = sqlite3_step(c) == SQLITE_DONE ? sqlite3_changes(banco) : -1;
    sqlite3_finalize(c);
    return resultado;
}

/* Importa um evento recebido. Devolve 1 se era novo, 0 se repetido, -1 em erro. */
static int importar(sqlite3 *banco, const msgpack_object *evento)
{
    char origem[256], chave[256];
    const Tabela *t = achar_tabela(buscar(evento, "tipo"));
    const msgpack_object *bruto = buscar(evento, "dados");
    if (t == NULL || !copiar_texto(buscar(evento, "origem"), origem, sizeof origem) ||
        !copiar_texto(buscar(evento, "chave"), chave, sizeof chave) || bruto == NULL ||
        bruto->type != MSGPACK_OBJECT_BIN) {
        avisar("evento de réplica inválido", NULL);
        return -1;
    }
    msgpack_unpacked dados;
    msgpack_unpacked_init(&dados);
    char chave_dados[256];
    int resultado = -1;
    if (decodificar(bruto->via.bin.ptr, bruto->via.bin.size, &dados) != 0 ||
        !colunas_conferem(t, &dados.data) ||
        !chave_do_valor(buscar(&dados.data, t->colunas[0]), chave_dados, sizeof chave_dados) ||
        strcmp(chave_dados, chave) != 0) {
        avisar("dados de réplica inválidos", t->nome);
        goto fim;
    }
    int novo = gravar_evento(banco, origem, t->nome, chave, bruto->via.bin.ptr, bruto->via.bin.size);
    if (novo <= 0) {
        resultado = novo;
        goto fim;
    }

    const msgpack_object *linha = &dados.data;
    if (strcmp(t->nome, "canais") == 0) {
        /* Mesmo canal criado em dois servidores: vence o menor (timestamp, origem). */
        const msgpack_object *instante = buscar(linha, "timestamp");
        if (!eh_inteiro(instante)) goto fim;
        int64_t tempo = instante->type == MSGPACK_OBJECT_POSITIVE_INTEGER ? (int64_t)instante->via.u64
                                                                          : instante->via.i64;
        sqlite3_stmt *c = NULL;
        if (sqlite3_prepare_v2(banco,
                               "SELECT timestamp, origem FROM replica_vencedor_canal WHERE nome=?", -1,
                               &c, NULL) != SQLITE_OK)
            goto fim;
        ligar_texto(c, 1, chave);
        bool vence = true;
        if (sqlite3_step(c) == SQLITE_ROW) {
            int64_t tempo_vencedor = sqlite3_column_int64(c, 0);
            const char *origem_vencedor = (const char *)sqlite3_column_text(c, 1);
            vence = tempo < tempo_vencedor ||
                    (tempo == tempo_vencedor && origem_vencedor != NULL &&
                     strcmp(origem, origem_vencedor) < 0);
        }
        sqlite3_finalize(c);
        if (vence) {
            sqlite3_stmt *v = NULL;
            if (alterar_com_dados(banco, "INSERT OR REPLACE INTO canais VALUES (?, ?, ?)", t, 0,
                                  linha) < 0 ||
                sqlite3_prepare_v2(banco,
                                   "INSERT OR REPLACE INTO replica_vencedor_canal VALUES (?, ?, ?)",
                                   -1, &v, NULL) != SQLITE_OK)
                goto fim;
            ligar_texto(v, 1, chave);
            sqlite3_bind_int64(v, 2, tempo);
            ligar_texto(v, 3, origem);
            int passo = sqlite3_step(v);
            sqlite3_finalize(v);
            if (passo != SQLITE_DONE ||
                alterar(banco, "INSERT OR IGNORE INTO replica_importados VALUES (?, ?)", t->nome,
                        chave, NULL) < 0)
                goto fim;
        }
    } else if (strcmp(t->nome, "publicacoes") == 0) {
        /* O UUID é a chave primária, então a mesma publicação entra uma vez só. */
        char colunas[256], marcadores[64], sql[512];
        listar_colunas(t, 0, ", ", "", colunas, sizeof colunas);
        listar_marcadores(t->quantidade, marcadores, sizeof marcadores);
        snprintf(sql, sizeof sql, "INSERT OR IGNORE INTO %s (%s) VALUES (%s)", t->nome, colunas,
                 marcadores);
        int inseridas = alterar_com_dados(banco, sql, t, 0, linha);
        if (inseridas < 0) goto fim;
        if (inseridas > 0 &&
            alterar(banco, "INSERT OR IGNORE INTO replica_importados VALUES (?, ?)", t->nome, chave,
                    NULL) < 0)
            goto fim;
    } else {
        /* logins e trocas: o id é local, então procuramos uma linha com os
         * mesmos valores antes de inserir com um id novo. */
        char filtro[256], colunas[256], marcadores[64], sql[768];
        listar_colunas(t, 1, " AND ", "=?", filtro, sizeof filtro);
        snprintf(sql, sizeof sql, "SELECT id FROM %s WHERE %s LIMIT 1", t->nome, filtro);
        sqlite3_stmt *c = NULL;
        if (sqlite3_prepare_v2(banco, sql, -1, &c, NULL) != SQLITE_OK) goto fim;
        bool ligado = true;
        for (int i = 1; i < t->quantidade; i++)
            if (ligar_objeto(c, i, buscar(linha, t->colunas[i])) != SQLITE_OK) ligado = false;
        int passo = ligado ? sqlite3_step(c) : SQLITE_ERROR;
        sqlite3_finalize(c);
        if (passo == SQLITE_DONE) {
            listar_colunas(t, 1, ", ", "", colunas, sizeof colunas);
            listar_marcadores(t->quantidade - 1, marcadores, sizeof marcadores);
            snprintf(sql, sizeof sql, "INSERT INTO %s (%s) VALUES (%s)", t->nome, colunas, marcadores);
            if (alterar_com_dados(banco, sql, t, 1, linha) < 0) goto fim;
            char id[32];
            snprintf(id, sizeof id, "%lld", (long long)sqlite3_last_insert_rowid(banco));
            if (alterar(banco, "INSERT OR IGNORE INTO replica_importados VALUES (?, ?)", t->nome, id,
                        NULL) < 0)
                goto fim;
        } else if (passo != SQLITE_ROW) {
            goto fim;
        }
    }
    resultado = 1;
fim:
    if (resultado < 0) avisar("falha ao importar", sqlite3_errmsg(banco));
    msgpack_unpacked_destroy(&dados);
    return resultado;
}

/* Responde {"desde": seq} com até LOTE eventos do log, em ordem de seq. */
static void responder(sqlite3 *banco, const void *dados, size_t tamanho)
{
    int64_t desde = 0;
    msgpack_unpacked pedido;
    msgpack_unpacked_init(&pedido);
    if (decodificar(dados, tamanho, &pedido) == 0) {
        const msgpack_object *valor = buscar(&pedido.data, "desde");
        if (valor != NULL && valor->type == MSGPACK_OBJECT_POSITIVE_INTEGER &&
            valor->via.u64 <= INT64_MAX)
            desde = (int64_t)valor->via.u64;
    }
    msgpack_unpacked_destroy(&pedido);

    /* Empacotamos os eventos à parte porque a lista precisa do tamanho antes. */
    msgpack_sbuffer itens, saida;
    msgpack_sbuffer_init(&itens);
    msgpack_sbuffer_init(&saida);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &itens, msgpack_sbuffer_write);
    static const char *campos[] = {"seq", "origem", "tipo", "chave", "dados"};
    size_t quantidade = 0;
    sqlite3_stmt *c = NULL;
    if (sqlite3_prepare_v2(banco,
                           "SELECT seq, origem, tipo, chave, dados FROM replica_eventos "
                           "WHERE seq>? ORDER BY seq LIMIT ?",
                           -1, &c, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(c, 1, desde);
        sqlite3_bind_int(c, 2, LOTE);
        while (sqlite3_step(c) == SQLITE_ROW) {
            msgpack_pack_map(&pk, 5);
            for (int i = 0; i < 5; i++) {
                empacotar_texto(&pk, campos[i], strlen(campos[i]));
                empacotar_coluna(&pk, c, i);
            }
            quantidade++;
        }
    }
    sqlite3_finalize(c);
    msgpack_packer_init(&pk, &saida, msgpack_sbuffer_write);
    msgpack_pack_map(&pk, 1);
    empacotar_texto(&pk, "eventos", 7);
    msgpack_pack_array(&pk, quantidade);
    msgpack_sbuffer_write(&saida, itens.data, itens.size);
    /* Até um pedido inválido recebe resposta, senão o REP trava. */
    zmq_send(rep, saida.data, saida.size, 0);
    msgpack_sbuffer_destroy(&itens);
    msgpack_sbuffer_destroy(&saida);
}

static void *ouvir(void *arg)
{
    (void)arg;
    sqlite3 *banco = conectar();
    while (!parar) {
        zmq_msg_t mensagem;
        zmq_msg_init(&mensagem);
        if (zmq_msg_recv(&mensagem, rep, 0) >= 0) {
            /* Lemos partes extras para o REP voltar ao estado de responder. */
            while (zmq_msg_more(&mensagem)) {
                zmq_msg_t extra;
                zmq_msg_init(&extra);
                int lido = zmq_msg_recv(&extra, rep, 0);
                int mais = lido >= 0 && zmq_msg_more(&extra);
                zmq_msg_close(&extra);
                if (!mais) break;
            }
            if (banco != NULL) {
                responder(banco, zmq_msg_data(&mensagem), zmq_msg_size(&mensagem));
            } else {
                zmq_send(rep, "\x81\xa7" "eventos" "\x90", 10, 0);
            }
        }
        zmq_msg_close(&mensagem);
    }
    sqlite3_close(banco);
    return NULL;
}

/* Pedido REQ/REP com prazo. Devolve 0 e preenche `resposta` (liberar com
 * msgpack_unpacked_destroy e zmq_msg_close) ou -1 sem resposta válida. */
static int requisitar(const char *destino, const msgpack_sbuffer *pedido, int prazo,
                      zmq_msg_t *bruto, msgpack_unpacked *resposta)
{
    int resultado = -1;
    msgpack_unpacked_init(resposta);
    zmq_msg_init(bruto);
    void *s = zmq_socket(contexto, ZMQ_REQ);
    if (s == NULL) return -1;
    int zero = 0;
    zmq_setsockopt(s, ZMQ_LINGER, &zero, sizeof zero);
    zmq_setsockopt(s, ZMQ_SNDTIMEO, &prazo, sizeof prazo);
    zmq_setsockopt(s, ZMQ_RCVTIMEO, &prazo, sizeof prazo);
    if (zmq_connect(s, destino) == 0 && zmq_send(s, pedido->data, pedido->size, 0) >= 0 &&
        zmq_msg_recv(bruto, s, 0) >= 0 &&
        decodificar(zmq_msg_data(bruto), zmq_msg_size(bruto), resposta) == 0 &&
        resposta->data.type == MSGPACK_OBJECT_MAP)
        resultado = 0;
    zmq_close(s);
    return resultado;
}

/* Pedido à referência com o envelope completo e o clock compartilhado. */
static int pedir_referencia(const char *operacao, bool com_nome, zmq_msg_t *bruto,
                            msgpack_unpacked *resposta)
{
    msgpack_sbuffer pedido;
    msgpack_sbuffer_init(&pedido);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &pedido, msgpack_sbuffer_write);
    int erro = msgpack_pack_map(&pk, 4);
    erro |= empacotar_texto(&pk, "operation", 9);
    erro |= empacotar_texto(&pk, operacao, strlen(operacao));
    erro |= empacotar_texto(&pk, "timestamp", 9);
    erro |= msgpack_pack_int64(&pk, timestamp_ms());
    erro |= empacotar_clock(&pk);
    erro |= empacotar_texto(&pk, "payload", 7);
    erro |= msgpack_pack_map(&pk, com_nome ? 1 : 0);
    if (com_nome) {
        erro |= empacotar_texto(&pk, "name", 4);
        erro |= empacotar_texto(&pk, nome, strlen(nome));
    }
    int resultado = -1;
    if (!erro && requisitar(endereco_referencia, &pedido, PRAZO_REFERENCIA_MS, bruto, resposta) == 0 &&
        relogio_receber(&resposta->data) == 0 &&
        texto_igual(buscar(&resposta->data, "operation"), operacao) &&
        texto_igual(buscar(&resposta->data, "status"), "ok"))
        resultado = 0;
    msgpack_sbuffer_destroy(&pedido);
    return resultado;
}

/* Busca e importa os eventos de um servidor, a partir do último cursor. */
static void sincronizar_par(sqlite3 *banco, const char *outro, const char *destino)
{
    int64_t desde = 0;
    sqlite3_stmt *c = NULL;
    if (sqlite3_prepare_v2(banco, "SELECT seq FROM replica_cursores WHERE servidor=?", -1, &c, NULL) ==
        SQLITE_OK) {
        ligar_texto(c, 1, outro);
        if (sqlite3_step(c) == SQLITE_ROW) desde = sqlite3_column_int64(c, 0);
    }
    sqlite3_finalize(c);

    for (int lote = 0; lote < MAX_LOTES && !parar; lote++) {
        msgpack_sbuffer pedido;
        msgpack_sbuffer_init(&pedido);
        msgpack_packer pk;
        msgpack_packer_init(&pk, &pedido, msgpack_sbuffer_write);
        msgpack_pack_map(&pk, 1);
        empacotar_texto(&pk, "desde", 5);
        msgpack_pack_int64(&pk, desde);
        zmq_msg_t bruto;
        msgpack_unpacked resposta;
        int ok = requisitar(destino, &pedido, PRAZO_PAR_MS, &bruto, &resposta);
        msgpack_sbuffer_destroy(&pedido);
        const msgpack_object *eventos = ok == 0 ? buscar(&resposta.data, "eventos") : NULL;
        size_t quantidade = eventos != NULL && eventos->type == MSGPACK_OBJECT_ARRAY
                                ? eventos->via.array.size
                                : 0;
        bool continuar = false;
        if (quantidade > 0) {
            /* Eventos e cursor entram na mesma transação: ou tudo ou nada. */
            bool erro = executar(banco, "BEGIN IMMEDIATE") != 0;
            int64_t ultimo = desde;
            for (size_t i = 0; i < quantidade && !erro; i++) {
                const msgpack_object *evento = &eventos->via.array.ptr[i];
                const msgpack_object *seq = buscar(evento, "seq");
                if (seq == NULL || seq->type != MSGPACK_OBJECT_POSITIVE_INTEGER ||
                    seq->via.u64 > INT64_MAX || (int64_t)seq->via.u64 <= ultimo ||
                    importar(banco, evento) < 0) {
                    erro = true;
                } else {
                    ultimo = (int64_t)seq->via.u64;
                }
            }
            if (!erro) {
                sqlite3_stmt *cursor = NULL;
                erro = sqlite3_prepare_v2(banco, "INSERT OR REPLACE INTO replica_cursores VALUES (?, ?)",
                                          -1, &cursor, NULL) != SQLITE_OK;
                if (!erro) {
                    ligar_texto(cursor, 1, outro);
                    sqlite3_bind_int64(cursor, 2, ultimo);
                    erro = sqlite3_step(cursor) != SQLITE_DONE;
                }
                sqlite3_finalize(cursor);
            }
            executar(banco, erro ? "ROLLBACK" : "COMMIT");
            if (!erro) {
                desde = ultimo;
                printf("[%s] RÉPLICA | %zu eventos recebidos de %s\n", nome, quantidade, outro);
                fflush(stdout);
                continuar = quantidade >= LOTE;
            }
        }
        msgpack_unpacked_destroy(&resposta);
        zmq_msg_close(&bruto);
        if (!continuar) break;
    }
}

static void sincronizar(sqlite3 *banco)
{
    zmq_msg_t bruto;
    msgpack_unpacked resposta;
    if (pedir_referencia("list_servers", false, &bruto, &resposta) == 0) {
        const msgpack_object *lista = buscar(buscar(&resposta.data, "payload"), "servers");
        for (uint32_t i = 0; lista != NULL && lista->type == MSGPACK_OBJECT_ARRAY &&
                             i < lista->via.array.size && !parar;
             i++) {
            char outro[256], endereco[256], destino[300];
            const msgpack_object *par = &lista->via.array.ptr[i];
            if (!copiar_texto(buscar(par, "name"), outro, sizeof outro) ||
                !copiar_texto(buscar(par, "address"), endereco, sizeof endereco) ||
                strcmp(outro, nome) == 0 || strncmp(endereco, "tcp://", 6) != 0)
                continue;
            /* O REP de réplica fica no mesmo host do controle, na porta 5561. */
            char *porta = strrchr(endereco, ':');
            if (porta == NULL || porta < endereco + 6) continue;
            *porta = '\0';
            snprintf(destino, sizeof destino, "%s:5561", endereco);
            sincronizar_par(banco, outro, destino);
        }
    }
    msgpack_unpacked_destroy(&resposta);
    zmq_msg_close(&bruto);
}

static void *trabalhar(void *arg)
{
    (void)arg;
    sqlite3 *banco = conectar();
    double proximo_heartbeat = 0;
    while (banco != NULL && !parar) {
        /* Mantém este servidor na lista ativa mesmo quando não há bots. */
        if (agora_monotonico() >= proximo_heartbeat) {
            zmq_msg_t bruto;
            msgpack_unpacked resposta;
            (void)pedir_referencia("heartbeat", true, &bruto, &resposta);
            msgpack_unpacked_destroy(&resposta);
            zmq_msg_close(&bruto);
            proximo_heartbeat = agora_monotonico() + INTERVALO_HEARTBEAT;
        }
        for (size_t i = 0; i < QUANTIDADE_TABELAS && !parar; i++) capturar_tabela(banco, &tabelas[i]);
        sincronizar(banco);
        /* Esperamos em passos curtos para encerrar rápido. */
        for (double fim = agora_monotonico() + intervalo; !parar && agora_monotonico() < fim;)
            dormir(0.05);
    }
    sqlite3_close(banco);
    return NULL;
}

int replicacao_iniciar(void *ctx, const char *nome_servidor, const char *arquivo_banco)
{
    contexto = ctx;
    nome = nome_servidor;
    arquivo = arquivo_banco;
    endereco_referencia = ambiente("ENDERECO_REFERENCIA", "tcp://localhost:5559");
    intervalo = strtod(ambiente("INTERVALO_REPLICA", "1"), NULL);
    if (!(intervalo > 0)) intervalo = 1;
    parar = false;

    sqlite3 *banco = conectar();
    if (banco == NULL || preparar(banco) != 0) {
        sqlite3_close(banco);
        return -1;
    }
    sqlite3_close(banco);

    /* O bind é feito aqui para a falha aparecer na inicialização; o socket
     * passa para a thread ouvinte e só ela o usa depois disso. */
    const char *bind = ambiente("ENDERECO_REPLICA_BIND", "tcp://*:5561");
    int zero = 0, prazo = 200;
    rep = zmq_socket(contexto, ZMQ_REP);
    if (rep == NULL || zmq_setsockopt(rep, ZMQ_LINGER, &zero, sizeof zero) != 0 ||
        zmq_setsockopt(rep, ZMQ_RCVTIMEO, &prazo, sizeof prazo) != 0 || zmq_bind(rep, bind) != 0) {
        avisar("falha no bind", zmq_strerror(zmq_errno()));
        if (rep != NULL) zmq_close(rep);
        rep = NULL;
        return -1;
    }
    if (pthread_create(&ouvinte, NULL, ouvir, NULL) != 0) {
        zmq_close(rep);
        rep = NULL;
        return -1;
    }
    if (pthread_create(&trabalhador, NULL, trabalhar, NULL) != 0) {
        parar = true;
        pthread_join(ouvinte, NULL);
        zmq_close(rep);
        rep = NULL;
        return -1;
    }
    iniciado = true;
    printf("[%s] RÉPLICA | ouvindo em %s\n", nome, bind);
    fflush(stdout);
    return 0;
}

void replicacao_encerrar(void)
{
    if (!iniciado) return;
    parar = true;
    pthread_join(trabalhador, NULL);
    pthread_join(ouvinte, NULL);
    zmq_close(rep);
    rep = NULL;
    iniciado = false;
}
