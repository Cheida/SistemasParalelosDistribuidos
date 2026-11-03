"""Replica os quatro conjuntos de dados SQLite entre servidores Python, Java e C.

Origem: banco local de cada servidor. Destino: bancos dos demais servidores.
Resultado: cópias idempotentes que convergem por sincronização periódica.
Execute no mesmo container do servidor: python3 /app/replicacao.py.
"""
import os
import sqlite3
import threading
import time

import msgpack
import zmq


# Parâmetros: todos os servidores usam o mesmo esquema e porta de replicação.
BANCO = os.environ.get("ARQUIVO_BANCO", "/app/dados/servidor.db")
REFERENCIA = os.environ.get("ENDERECO_REFERENCIA", "tcp://referencia:5559")
NOME = os.environ.get("NOME_SERVIDOR") or f"servpy-{os.environ.get('HOSTNAME', 'local')[:12]}"
PORTA = os.environ.get("ENDERECO_REPLICA_BIND", "tcp://*:5561")
INTERVALO = float(os.environ.get("INTERVALO_REPLICA", "1"))
LOTE = 100

COLUNAS = {
    "logins": ("id", "username", "timestamp"),
    "canais": ("nome", "criado_por", "timestamp"),
    "publicacoes": ("id", "username", "canal", "mensagem", "timestamp_pedido", "timestamp_publicacao", "evento"),
    "trocas": ("id", "pedido", "resposta", "timestamp_recebimento"),
}


def conectar():
    banco = sqlite3.connect(BANCO, timeout=5)
    banco.execute("PRAGMA busy_timeout=5000")
    return banco


def preparar(banco):
    # O servidor cria as tabelas de aplicação; aguardamos sua inicialização.
    while True:
        tabelas = {linha[0] for linha in banco.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if set(COLUNAS).issubset(tabelas):
            break
        time.sleep(0.2)
    banco.executescript("""
        CREATE TABLE IF NOT EXISTS replica_eventos (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            origem TEXT NOT NULL,
            tipo TEXT NOT NULL,
            chave TEXT NOT NULL,
            dados BLOB NOT NULL,
            UNIQUE (origem, tipo, chave)
        );
        CREATE INDEX IF NOT EXISTS idx_replica_evento_chave ON replica_eventos (tipo, chave);
        CREATE TABLE IF NOT EXISTS replica_capturados (
            tipo TEXT NOT NULL,
            chave_local TEXT NOT NULL,
            PRIMARY KEY (tipo, chave_local)
        );
        CREATE TABLE IF NOT EXISTS replica_importados (
            tipo TEXT NOT NULL,
            chave_local TEXT NOT NULL,
            PRIMARY KEY (tipo, chave_local)
        );
        CREATE TABLE IF NOT EXISTS replica_cursores (
            servidor TEXT PRIMARY KEY,
            seq INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS replica_vencedor_canal (
            nome TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            origem TEXT NOT NULL
        );
    """)
    # Migração: versões anteriores já tinham eventos, mas não um índice local estável.
    # A comparação dos bytes evita confundir IDs iguais de servidores diferentes.
    with banco:
        for tipo, colunas in COLUNAS.items():
            chave = colunas[0]
            for linha in banco.execute(f"SELECT {', '.join(colunas)} FROM {tipo}"):
                dados = dict(zip(colunas, linha))
                identificador = str(dados[chave])
                bruto = msgpack.packb(dados, use_bin_type=True)
                existe = banco.execute(
                    "SELECT 1 FROM replica_eventos WHERE tipo=? AND chave=? AND dados=? LIMIT 1",
                    (tipo, identificador, bruto),
                ).fetchone()
                if existe:
                    banco.execute("INSERT OR IGNORE INTO replica_capturados VALUES (?,?)", (tipo, identificador))


def capturar_locais(banco):
    """Registra linhas originais; linhas importadas não viram eventos novos."""
    total = 0
    for tipo, colunas in COLUNAS.items():
        chave = colunas[0]
        linhas = banco.execute(
            f"SELECT {', '.join(colunas)} FROM {tipo} AS t "
            f"WHERE NOT EXISTS (SELECT 1 FROM replica_importados i WHERE i.tipo=? AND i.chave_local=CAST(t.{chave} AS TEXT)) "
            f"AND NOT EXISTS (SELECT 1 FROM replica_capturados c WHERE c.tipo=? AND c.chave_local=CAST(t.{chave} AS TEXT)) "
            f"AND NOT EXISTS (SELECT 1 FROM replica_eventos e WHERE e.origem=? AND e.tipo=? AND e.chave=CAST(t.{chave} AS TEXT)) "
            f"LIMIT ?", (tipo, tipo, NOME, tipo, LOTE),
        ).fetchall()
        with banco:
            for linha in linhas:
                dados = dict(zip(colunas, linha))
                identificador = str(dados[chave])
                banco.execute(
                    "INSERT OR IGNORE INTO replica_eventos (origem,tipo,chave,dados) VALUES (?,?,?,?)",
                    (NOME, tipo, identificador, msgpack.packb(dados, use_bin_type=True)),
                )
                banco.execute("INSERT OR IGNORE INTO replica_capturados VALUES (?,?)", (tipo, identificador))
                if tipo == "canais":
                    banco.execute(
                        "INSERT OR IGNORE INTO replica_vencedor_canal VALUES (?,?,?)",
                        (identificador, dados["timestamp"], NOME),
                    )
                total += 1
    return total


def importar(banco, evento):
    origem, tipo, chave, bruto = (evento[k] for k in ("origem", "tipo", "chave", "dados"))
    if tipo not in COLUNAS or not isinstance(origem, str) or not isinstance(chave, str) or not isinstance(bruto, bytes):
        raise ValueError("Evento de réplica inválido")
    dados = msgpack.unpackb(bruto, raw=False)
    if not isinstance(dados, dict) or set(dados) != set(COLUNAS[tipo]) or str(dados[COLUNAS[tipo][0]]) != chave:
        raise ValueError("Dados de réplica inválidos")
    inserido = banco.execute(
        "INSERT OR IGNORE INTO replica_eventos (origem,tipo,chave,dados) VALUES (?,?,?,?)",
        (origem, tipo, chave, bruto),
    ).rowcount
    if not inserido:
        return False
    if tipo == "canais":
        vencedor = banco.execute(
            "SELECT timestamp, origem FROM replica_vencedor_canal WHERE nome=?", (chave,)
        ).fetchone()
        if vencedor is None:
            # Pode haver um canal local ainda não capturado; a varredura o registra antes da importação.
            banco.execute("INSERT OR IGNORE INTO canais VALUES (?,?,?)",
                          (dados["nome"], dados["criado_por"], dados["timestamp"]))
        if vencedor is None or (dados["timestamp"], origem) < vencedor:
            banco.execute("INSERT OR REPLACE INTO canais VALUES (?,?,?)",
                          (dados["nome"], dados["criado_por"], dados["timestamp"]))
            banco.execute("INSERT OR REPLACE INTO replica_vencedor_canal VALUES (?,?,?)",
                          (chave, dados["timestamp"], origem))
            banco.execute("INSERT OR IGNORE INTO replica_importados VALUES (?,?)", (tipo, chave))
    elif tipo == "publicacoes":
        colunas = COLUNAS[tipo]
        cursor = banco.execute(
            f"INSERT OR IGNORE INTO {tipo} ({', '.join(colunas)}) VALUES ({', '.join('?' for _ in colunas)})",
            tuple(dados[c] for c in colunas),
        )
        if cursor.rowcount:
            banco.execute("INSERT OR IGNORE INTO replica_importados VALUES (?,?)", (tipo, chave))
    else:
        colunas = COLUNAS[tipo][1:]
        valores = tuple(dados[c] for c in colunas)
        existente = banco.execute(
            f"SELECT id FROM {tipo} WHERE " + " AND ".join(f"{c}=?" for c in colunas) + " LIMIT 1",
            valores,
        ).fetchone()
        if existente is None:
            cursor = banco.execute(
                f"INSERT INTO {tipo} ({', '.join(colunas)}) VALUES ({', '.join('?' for _ in colunas)})",
                valores,
            )
            banco.execute("INSERT OR IGNORE INTO replica_importados VALUES (?,?)",
                          (tipo, str(cursor.lastrowid)))
    return True


def requisitar(contexto, endereco, mensagem, prazo=350):
    socket = contexto.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, prazo)
    socket.setsockopt(zmq.RCVTIMEO, prazo)
    try:
        socket.connect(endereco)
        socket.send(msgpack.packb(mensagem, use_bin_type=True))
        return msgpack.unpackb(socket.recv(), raw=False)
    except (zmq.Again, zmq.ZMQError, ValueError, TypeError, msgpack.exceptions.UnpackException):
        return None
    finally:
        socket.close()


def atender(contexto):
    banco = conectar()
    socket = contexto.socket(zmq.REP)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind(PORTA)
    print(f"[{NOME}] RÉPLICA | ouvindo em {PORTA}", flush=True)
    try:
        while True:
            pedido = msgpack.unpackb(socket.recv(), raw=False)
            inicio = pedido.get("desde", 0) if isinstance(pedido, dict) else 0
            if type(inicio) is not int or inicio < 0:
                inicio = 0
            linhas = banco.execute(
                "SELECT seq,origem,tipo,chave,dados FROM replica_eventos WHERE seq>? ORDER BY seq LIMIT ?",
                (inicio, LOTE),
            ).fetchall()
            eventos = [dict(zip(("seq", "origem", "tipo", "chave", "dados"), linha)) for linha in linhas]
            socket.send(msgpack.packb({"eventos": eventos}, use_bin_type=True))
    finally:
        socket.close()
        banco.close()


def sincronizar(contexto, banco):
    resposta = requisitar(contexto, REFERENCIA, {
        "operation": "list_servers", "payload": {},
        "timestamp": int(time.time() * 1000), "clock": 1,
    })
    if not isinstance(resposta, dict) or resposta.get("status") != "ok":
        return
    for par in resposta.get("payload", {}).get("servers", []):
        nome, endereco = par.get("name"), par.get("address")
        if nome == NOME or not isinstance(nome, str) or not isinstance(endereco, str) or not endereco.startswith("tcp://"):
            continue
        destino = endereco.rsplit(":", 1)[0] + ":5561"
        cursor = banco.execute("SELECT seq FROM replica_cursores WHERE servidor=?", (nome,)).fetchone()
        desde = cursor[0] if cursor else 0
        for _ in range(10):
            lote = requisitar(contexto, destino, {"desde": desde})
            if not isinstance(lote, dict) or not isinstance(lote.get("eventos"), list):
                break
            eventos = lote["eventos"]
            if not eventos:
                break
            with banco:
                for evento in eventos:
                    importar(banco, evento)
                    desde = evento["seq"]
                banco.execute("INSERT OR REPLACE INTO replica_cursores VALUES (?,?)", (nome, desde))
            print(f"[{NOME}] RÉPLICA | {len(eventos)} eventos recebidos de {nome}", flush=True)
            if len(eventos) < LOTE:
                break


def main():
    banco = conectar()
    preparar(banco)
    contexto = zmq.Context()
    threading.Thread(target=atender, args=(contexto,), daemon=True).start()
    ultimo_heartbeat = 0.0
    try:
        while True:
            try:
                # Mantém este servidor na lista ativa mesmo quando não há bots.
                if time.monotonic() - ultimo_heartbeat >= 20:
                    requisitar(contexto, REFERENCIA, {
                        "operation": "heartbeat", "payload": {"name": NOME},
                        "timestamp": int(time.time() * 1000), "clock": 1,
                    })
                    ultimo_heartbeat = time.monotonic()
                capturar_locais(banco)
                sincronizar(contexto, banco)
            except (sqlite3.Error, ValueError, KeyError, TypeError) as erro:
                print(f"[{NOME}] RÉPLICA | erro: {erro}", flush=True)
            time.sleep(INTERVALO)
    finally:
        banco.close()
        contexto.term()


if __name__ == "__main__":
    main()
