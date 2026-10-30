"""Referência de ranks e disponibilidade. Na parte 4, não sincroniza relógios.
Só os servidores usam esse serviço. Execute: python python/referencia.py
"""
import os
import signal
import sqlite3
import time
from pathlib import Path
import msgpack
import zmq
from relogio import Relogio

endereco = os.getenv("ENDERECO_REFERENCIA_BIND", "tcp://*:5559")
arquivo_banco = os.getenv("BANCO_REFERENCIA", "dados/referencia.db")
prazo_heartbeat = float(os.getenv("PRAZO_HEARTBEAT", "60"))
if prazo_heartbeat <= 0:
    raise ValueError("PRAZO_HEARTBEAT precisa ser maior que zero")


def abrir_banco():
    Path(arquivo_banco).parent.mkdir(parents=True, exist_ok=True)
    banco = sqlite3.connect(arquivo_banco)
    # Os ranks continuam reservados após expiração e reinício da referência.
    banco.execute("""CREATE TABLE IF NOT EXISTS servidores (
        rank INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL UNIQUE,
        endereco TEXT NOT NULL
    )""")
    banco.commit()
    return banco


def remover_indisponiveis(ativos, agora):
    # monotonic não é afetado por correções do relógio físico.
    for nome in list(ativos):
        if agora - ativos[nome] >= prazo_heartbeat:
            del ativos[nome]
            print(f"[referencia] INDISPONÍVEL | servidor: {nome}", flush=True)


def tratar_pedido(pedido, banco, ativos):
    operacao = pedido.get("operation")
    dados = pedido.get("payload")
    if (not isinstance(operacao, str) or not isinstance(dados, dict)
            or type(pedido.get("timestamp")) is not int
            or not 0 <= pedido["timestamp"] <= 2**63 - 1):
        return {}, "invalid_request"
    agora = time.monotonic()
    remover_indisponiveis(ativos, agora)

    if operacao == "list_servers":
        linhas = banco.execute("SELECT nome, rank, endereco FROM servidores ORDER BY rank").fetchall()
        return {"servers": [
            {"name": nome, "rank": rank, "address": endereco_servidor}
            for nome, rank, endereco_servidor in linhas if nome in ativos
        ]}, "ok"

    if operacao not in ("register_server", "heartbeat"):
        return {}, "unknown_operation"
    nome = dados.get("name")
    if not isinstance(nome, str) or not 1 <= len(nome) <= 64:
        return {}, "invalid_name"
    linha = banco.execute("SELECT rank, endereco FROM servidores WHERE nome = ?", (nome,)).fetchone()

    if operacao == "register_server":
        endereco_servidor = dados.get("address")
        if not isinstance(endereco_servidor, str) or not endereco_servidor:
            return {}, "invalid_request"
        if linha is None:
            with banco:
                cursor = banco.execute(
                    "INSERT INTO servidores (nome, endereco) VALUES (?, ?)", (nome, endereco_servidor)
                )
            rank = cursor.lastrowid
        else:
            rank = linha[0]
            with banco:
                banco.execute("UPDATE servidores SET endereco = ? WHERE nome = ?", (endereco_servidor, nome))
        ativos[nome] = agora
        return {"name": nome, "rank": rank}, "ok"

    if linha is None:
        # Isso permite ao servidor perceber que precisa se registrar de novo.
        return {}, "server_not_registered"
    ativos[nome] = agora
    # O timestamp do envelope continua obrigatório, mas não é fonte de sincronização.
    # Quem fornece o horário sincronizado agora é o coordenador eleito.
    return {"name": nome, "rank": linha[0]}, "ok"


def main():
    banco = abrir_banco()
    contexto = zmq.Context()
    socket = contexto.socket(zmq.REP)
    socket.bind(endereco)
    relogio = Relogio()
    ativos = {}

    def encerrar(sinal, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, encerrar)
    print(f"[referencia] Iniciada | {endereco} | prazo: {prazo_heartbeat}s", flush=True)
    try:
        while True:
            # Mesmo sem pedidos, retiramos os servidores que expiraram.
            remover_indisponiveis(ativos, time.monotonic())
            if not socket.poll(200):
                continue
            pedido = {}
            try:
                pedido = msgpack.unpackb(socket.recv(), raw=False)
                relogio.receber(pedido)
                print(f"[referencia] RECEBENDO | {pedido}", flush=True)
                dados, status = tratar_pedido(pedido, banco, ativos)
            except (ValueError, TypeError, msgpack.exceptions.UnpackException):
                dados, status = {}, "invalid_request"
            except sqlite3.Error as erro:
                print(f"[referencia] Falha no banco: {erro}", flush=True)
                dados, status = {}, "internal_error"
            operacao = pedido.get("operation", "") if isinstance(pedido, dict) else ""
            resposta = {
                "operation": operacao if isinstance(operacao, str) else "",
                "status": status,
                "payload": dados,
            }
            relogio.preparar_envio(resposta)
            print(f"[referencia] {resposta}", flush=True)
            socket.send(msgpack.packb(resposta, use_bin_type=True))
    except KeyboardInterrupt:
        print("Referência encerrada.", flush=True)
    finally:
        socket.close(linger=0)
        contexto.term()
        banco.close()


if __name__ == "__main__":
    main()
