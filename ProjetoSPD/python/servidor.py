"""Recebe pedidos em MessagePack e salva logins e canais em SQLite.
Execute na pasta do projeto: python python/servidor.py
"""
import os
import uuid
import re
import sqlite3
import time
from pathlib import Path
import msgpack
import zmq

# Cada servidor pode receber um nome e um caminho de banco próprios.
endereco_broker = os.getenv("ENDERECO_BROKER", "tcp://localhost:5556")
# Cada réplica recebe uma identificação diferente nos registros.
identificador = os.getenv("HOSTNAME")
nome_padrao = f"servpy-{identificador[:12]}" if identificador else "servpy"
nome_servidor = os.getenv("NOME_SERVIDOR", nome_padrao)
endereco_publicacao = os.getenv("ENDERECO_PUBLICACAO", "tcp://localhost:5557")
pasta_projeto = Path(__file__).resolve().parent.parent
arquivo_banco = os.getenv(
    "ARQUIVO_BANCO", str(pasta_projeto / "dados" / f"{nome_servidor}.db")
)
# fullmatch exige que o nome inteiro tenha de 1 a 32 caracteres permitidos.
padrao_nome = re.compile(r"[A-Za-z0-9_-]{1,32}")
# Pausa visual da demonstração; use PAUSA_VISUAL=0 para desativar.
pausa_visual = max(0, min(float(os.getenv("PAUSA_VISUAL", "0")), 1))


def mostrar_mensagem(acao, mensagem):
    # Exibimos o timestamp transmitido, junto da operação e dos dados.
    if not isinstance(mensagem, dict):
        print(f"[{nome_servidor}] {acao} | mensagem inválida", flush=True)
        return

    nomes = {
        "login": "login",
        "list_channels": "listar canais",
        "create_channel": "criar canal",
        "publish": "publicar",
    }
    operacao = mensagem.get("operation", "desconhecida")
    partes = [nomes.get(operacao, operacao), f"timestamp: {mensagem.get('timestamp', 'ausente')}"]
    dados = mensagem.get("payload", {})
    if isinstance(dados, dict):
        if "username" in dados:
            partes.append(f"usuário: {dados['username']}")
        if "channel" in dados:
            partes.append(f"canal: {dados['channel']}")
        if "channels" in dados:
            canais = dados["channels"]
            if isinstance(canais, list):
                partes.append("canais: " + (", ".join(map(str, canais)) or "nenhum"))
    if isinstance(dados, dict) and "message" in dados:
        partes.append(f"mensagem: {dados['message']}")
    if dados == {}:
        partes.append("dados: {}")
    if "status" in mensagem:
        partes.append(f"status: {mensagem['status']}")
    print(f"[{nome_servidor}] {acao} | " + " | ".join(partes), flush=True)

    # Para conferir todos os campos na apresentação, ative MENSAGENS_COMPLETAS=1.
    if os.getenv("MENSAGENS_COMPLETAS", "0") == "1":
        print(f"    conteúdo: {mensagem}", flush=True)


def timestamp():
    # Usamos a mesma unidade do cliente: milissegundos desde Unix epoch.
    return time.time_ns() // 1_000_000


def abrir_banco():
    # Criamos a pasta do banco se ainda não existir.
    Path(arquivo_banco).parent.mkdir(parents=True, exist_ok=True)
    banco = sqlite3.connect(arquivo_banco)
    # As tabelas existentes são preservadas ao reiniciar o servidor.
    banco.executescript("""
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS canais (
            nome TEXT PRIMARY KEY,
            criado_por TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );
    """)
    # Guardamos o evento publicado e os pedidos/respostas completos em binário.
    # Assim dá para recuperar os dados sem depender do que apareceu no terminal.
    banco.executescript("""
        CREATE TABLE IF NOT EXISTS publicacoes (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            canal TEXT NOT NULL,
            mensagem TEXT NOT NULL,
            timestamp_pedido INTEGER NOT NULL,
            timestamp_publicacao INTEGER NOT NULL,
            evento BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trocas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido BLOB NOT NULL,
            resposta BLOB NOT NULL,
            timestamp_recebimento INTEGER NOT NULL
        );
    """)
    banco.commit()
    return banco


def nome_valido(nome):
    return isinstance(nome, str) and padrao_nome.fullmatch(nome) is not None


def montar_resposta(pedido, payload=None, erro=None):
    return {
        "operation": pedido.get("operation", "") if isinstance(pedido.get("operation", ""), str) else "",
        "timestamp": timestamp(),
        "status": erro.lower() if erro else "ok",
        "payload": {} if payload is None else payload,
    }


def tratar_pedido(pedido, banco, publicador=None):
    # Antes de executar a operação, conferimos os campos e seus tipos.
    campos = {"operation", "timestamp", "payload"}
    if not campos.issubset(pedido):
        return montar_resposta(pedido, erro="INVALID_REQUEST")
    if (
        not isinstance(pedido["operation"], str)
        or type(pedido["timestamp"]) is not int
        or not 0 <= pedido["timestamp"] <= 2**63 - 1
        or not isinstance(pedido["payload"], dict)
    ):
        return montar_resposta(pedido, erro="INVALID_REQUEST")
    operacao = pedido["operation"]
    dados = pedido["payload"]
    if operacao in ("login", "create_channel", "publish"):
        usuario = dados.get("username")
        if not isinstance(usuario, str):
            return montar_resposta(pedido, erro="INVALID_REQUEST")
        if not nome_valido(usuario):
            return montar_resposta(pedido, erro="INVALID_NAME")

    if operacao == "login":
        # Salvamos cada login recebido, inclusive nomes repetidos.
        # O bloco confirma a gravação ao terminar e desfaz se ocorrer um erro.
        with banco:
            banco.execute(
                "INSERT INTO logins (username, timestamp) VALUES (?, ?)",
                (usuario, pedido["timestamp"]),
            )
        return montar_resposta(pedido, {"username": usuario})

    if operacao == "create_channel":
        canal = dados.get("channel")
        if not isinstance(canal, str):
            return montar_resposta(pedido, erro="INVALID_REQUEST")
        if not nome_valido(canal):
            return montar_resposta(pedido, erro="INVALID_NAME")
        try:
            # Os parâmetros evitam misturar os dados com o comando SQL.
            with banco:
                banco.execute(
                    "INSERT INTO canais (nome, criado_por, timestamp) VALUES (?, ?, ?)",
                    (canal, usuario, pedido["timestamp"]),
                )
        # A chave primária do canal impede cadastrar o mesmo nome duas vezes.
        except sqlite3.IntegrityError:
            return montar_resposta(pedido, erro="CHANNEL_EXISTS")
        return montar_resposta(pedido, {"channel": canal})

    if operacao == "publish":
        canal = dados.get("channel")
        texto = dados.get("message")
        if not isinstance(canal, str) or not isinstance(texto, str):
            return montar_resposta(pedido, erro="INVALID_REQUEST")
        if not nome_valido(canal):
            return montar_resposta(pedido, erro="INVALID_NAME")
        if not texto.strip() or len(texto) > 500:
            return montar_resposta(pedido, erro="INVALID_MESSAGE")
        if publicador is None:
            return montar_resposta(pedido, erro="INTERNAL_ERROR")

        # O canal pode ter sido criado em outro servidor. Sem replicação,
        # não usamos a lista local para recusar um canal escolhido pelo bot.
        identificador_evento = str(uuid.uuid4())
        evento = {
            "operation": "publication",
            "timestamp": timestamp(),
            "payload": {
                "id": identificador_evento,
                "username": usuario,
                "channel": canal,
                "message": texto,
                "request_timestamp": pedido["timestamp"],
            },
        }
        evento_binario = msgpack.packb(evento, use_bin_type=True)
        # Primeiro salvamos em disco; só depois tentamos publicar.
        with banco:
            banco.execute(
                """INSERT INTO publicacoes
                (id, username, canal, mensagem, timestamp_pedido,
                 timestamp_publicacao, evento) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (identificador_evento, usuario, canal, texto,
                 pedido["timestamp"], evento["timestamp"], evento_binario),
            )
        # Dois frames binários: nome do tópico em MessagePack e evento.
        # O prefixo de tamanho do MessagePack distingue canal1 de canal10.
        publicador.send_multipart([
            msgpack.packb(canal, use_bin_type=True), evento_binario
        ])
        mostrar_mensagem("PUBLICANDO", evento)
        return montar_resposta(pedido, {"channel": canal, "id": identificador_evento})

    if operacao == "list_channels":
        # Buscamos os canais em ordem alfabética e devolvemos só os nomes.
        registros = banco.execute("SELECT nome FROM canais ORDER BY nome").fetchall()
        return montar_resposta(pedido, {"channels": [linha[0] for linha in registros]})
    return montar_resposta(pedido, erro="UNKNOWN_OPERATION")


def main():
    banco = abrir_banco()
    contexto = zmq.Context()
    # REP deve responder ao pedido atual antes de receber outro.
    socket = contexto.socket(zmq.REP)
    socket.connect(endereco_broker)
    publicador = contexto.socket(zmq.PUB)
    publicador.connect(endereco_publicacao)
    print(f"{nome_servidor} iniciado. Banco: {arquivo_banco}", flush=True)
    try:
        while True:
            # Aqui aguardamos um pedido, sem gastar CPU consultando em loop.
            mensagem = socket.recv()
            recebido_em = timestamp()
            pedido = {}
            try:
                # Transformamos os bytes recebidos em estruturas do Python.
                pedido = msgpack.unpackb(mensagem, raw=False)
            except (ValueError, TypeError, msgpack.exceptions.UnpackException):
                print(f"Mensagem inválida: {mensagem!r}", flush=True)
                # Até um pacote inválido recebe resposta para manter o ciclo do REP.
                resposta = montar_resposta({}, erro="INVALID_MESSAGE")
            else:
                # Separamos visualmente o envio do bot e o recebimento aqui.
                time.sleep(pausa_visual)
                mostrar_mensagem("RECEBENDO", pedido)
                time.sleep(pausa_visual)
                if not isinstance(pedido, dict):
                    resposta = montar_resposta({}, erro="INVALID_REQUEST")
                else:
                    try:
                        resposta = tratar_pedido(pedido, banco, publicador)
                    except (sqlite3.Error, zmq.ZMQError) as erro:
                        print(f"Erro no banco: {erro}", flush=True)
                        resposta = montar_resposta(pedido, erro="INTERNAL_ERROR")
            # REP precisa responder antes de receber outro pedido.
            # Atualizamos o horário depois das pausas, perto do envio.
            resposta["timestamp"] = timestamp()
            # Também registramos a requisição e a resposta completas.
            try:
                with banco:
                    banco.execute(
                        "INSERT INTO trocas (pedido, resposta, timestamp_recebimento) VALUES (?, ?, ?)",
                        (mensagem, msgpack.packb(resposta, use_bin_type=True), recebido_em),
                    )
            except sqlite3.Error as erro:
                print(f"Falha ao registrar troca: {erro}", flush=True)
                resposta = montar_resposta(pedido if isinstance(pedido, dict) else {}, erro="INTERNAL_ERROR")
            mostrar_mensagem("RESPONDENDO", resposta)
            socket.send(msgpack.packb(resposta, use_bin_type=True))
    except KeyboardInterrupt:
        print("Servidor encerrado.", flush=True)
    finally:
        publicador.close(linger=0)
        socket.close(linger=0)
        contexto.term()
        # Fechamos o banco para liberar os recursos usados pelo servidor.
        banco.close()


if __name__ == "__main__":
    main()
