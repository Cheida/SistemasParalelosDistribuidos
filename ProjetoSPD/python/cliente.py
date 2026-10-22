"""Bot das partes 1 e 2: login, canais, inscrições e publicações automáticas.
Execute: python python/cliente.py. Os endereços também podem vir do Compose.
"""
import os
import random
import time
import msgpack
import zmq

endereco_broker = os.getenv("ENDERECO_BROKER", "tcp://localhost:5555")
endereco_inscricao = os.getenv("ENDERECO_INSCRICAO", "tcp://localhost:5558")
identificador = os.getenv("HOSTNAME", "local")
nome_bot = os.getenv("NOME_BOT", f"bot-py-{identificador[:12]}")
tempo_resposta_ms = 5000
intervalo_tentativas = 2
intervalo_mensagens = 1
frases = [
    "Bom dia, pessoal!", "Alguém está por aqui?", "Teste de publicação.",
    "Mensagem enviada pelo bot.", "Tudo certo por aqui!", "Olá, canal!",
]


def timestamp():
    # Timestamp do protocolo, em milissegundos desde Unix epoch.
    return time.time_ns() // 1_000_000


def mostrar_mensagem(acao, mensagem):
    dados = mensagem.get("payload", {})
    partes = [mensagem.get("operation", "desconhecida"),
              f"timestamp: {mensagem.get('timestamp')}"]
    for campo, rotulo in [("username", "usuário"), ("channel", "canal"),
                          ("message", "mensagem")]:
        if campo in dados:
            partes.append(f"{rotulo}: {dados[campo]}")
    if "channels" in dados:
        partes.append("canais: " + (", ".join(dados["channels"]) or "nenhum"))
    if "status" in mensagem:
        partes.append(f"status: {mensagem['status']}")
    print(f"[{nome_bot}] {acao} | " + " | ".join(partes), flush=True)
    if os.getenv("MENSAGENS_COMPLETAS", "0") == "1":
        print(f"    conteúdo: {mensagem}", flush=True)


def receber_publicacoes(inscrito):
    # O mesmo SUB recebe de todos os canais em que este bot se inscreveu.
    # Limitamos cada passada para não deixar os pedidos esperando para sempre.
    for _ in range(100):
        try:
            frames = inscrito.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.Again:
            return
        recebido_em = timestamp()
        try:
            if len(frames) != 2:
                raise ValueError("A publicação precisa ter dois frames")
            canal = msgpack.unpackb(frames[0], raw=False)
            evento = msgpack.unpackb(frames[1], raw=False)
            dados = evento["payload"]
            if (evento["operation"] != "publication"
                    or canal != dados["channel"]
                    or type(evento["timestamp"]) is not int
                    or not isinstance(dados["message"], str)):
                raise ValueError("Publicação fora do protocolo")
            print(
                f"[{nome_bot}] PUBLICAÇÃO | canal: {canal} | "
                f"usuário: {dados['username']} | mensagem: {dados['message']} | "
                f"envio: {evento['timestamp']} | recebimento: {recebido_em}",
                flush=True,
            )
            if os.getenv("MENSAGENS_COMPLETAS", "0") == "1":
                print(f"    conteúdo: {evento}", flush=True)
        except (ValueError, TypeError, KeyError, msgpack.exceptions.UnpackException):
            print(f"[{nome_bot}] Publicação inválida recebida.", flush=True)


def esperar(inscrito, segundos):
    # Durante uma pausa, continuamos recebendo mensagens dos canais.
    fim = time.monotonic() + segundos
    while True:
        receber_publicacoes(inscrito)
        restante = fim - time.monotonic()
        if restante <= 0:
            return
        inscrito.poll(timeout=max(1, min(100, int(restante * 1000))))


def fazer_pedido(contexto, inscrito, operacao, dados):
    # Um REQ novo permite tentar novamente depois de um timeout.
    pedido = {"operation": operacao, "timestamp": timestamp(), "payload": dados}
    socket = contexto.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, tempo_resposta_ms)
    socket.connect(endereco_broker)
    try:
        mostrar_mensagem("ENVIANDO", pedido)
        socket.send(msgpack.packb(pedido, use_bin_type=True))
        fim = time.monotonic() + tempo_resposta_ms / 1000
        while time.monotonic() < fim:
            # Não bloqueamos o SUB enquanto esperamos a resposta do servidor.
            receber_publicacoes(inscrito)
            if socket.poll(timeout=50):
                resposta = msgpack.unpackb(socket.recv(), raw=False)
                if (not isinstance(resposta, dict)
                        or resposta.get("operation") != operacao
                        or type(resposta.get("timestamp")) is not int
                        or not isinstance(resposta.get("status"), str)
                        or not isinstance(resposta.get("payload"), dict)):
                    raise ValueError("Resposta fora do protocolo")
                mostrar_mensagem("RECEBENDO", resposta)
                return resposta
        print(f"[{nome_bot}] Tempo de resposta esgotado.", flush=True)
    except (zmq.Again, ValueError, TypeError, msgpack.exceptions.UnpackException):
        print(f"[{nome_bot}] Não foi possível concluir o pedido.", flush=True)
    finally:
        socket.close()
    return None


def listar_canais(contexto, inscrito):
    resposta = fazer_pedido(contexto, inscrito, "list_channels", {})
    if resposta is not None and resposta["status"] == "ok":
        canais = resposta["payload"].get("channels")
        if isinstance(canais, list) and all(isinstance(c, str) for c in canais):
            return canais
    return None


def preparar_canais(contexto, inscrito, assinaturas):
    canais = listar_canais(contexto, inscrito)
    if canais is None:
        return []
    if len(canais) < 5:
        # Um nome novo por passagem, até que a lista tenha pelo menos cinco.
        numeros = [int(c[5:]) for c in canais
                   if c.startswith("canal") and c[5:].isascii() and c[5:].isdigit()]
        canal = f"canal{max(numeros, default=0) + 1}"
        criacao = fazer_pedido(contexto, inscrito, "create_channel", {
            "username": nome_bot, "channel": canal,
        })
        # Só adicionamos à lista um canal que o servidor confirmou.
        if criacao is not None and criacao["status"] in ("ok", "channel_exists"):
            canais = sorted(set(canais + [canal]))
    if len(assinaturas) < 3:
        opcoes = [c for c in canais if c not in assinaturas]
        if opcoes:
            canal = random.choice(opcoes)
            # O tópico também usa MessagePack, como o primeiro frame do PUB.
            inscrito.setsockopt(zmq.SUBSCRIBE, msgpack.packb(canal, use_bin_type=True))
            assinaturas.add(canal)
            print(f"[{nome_bot}] INSCRITO | canal: {canal}", flush=True)
            # Damos tempo para a inscrição atravessar o proxy (slow joiner).
            esperar(inscrito, 1)
    return canais


def main():
    contexto = zmq.Context()
    inscrito = contexto.socket(zmq.SUB)
    inscrito.connect(endereco_inscricao)
    assinaturas = set()
    try:
        while True:
            resposta = fazer_pedido(contexto, inscrito, "login", {"username": nome_bot})
            if resposta is not None and resposta["status"] == "ok":
                break
            esperar(inscrito, intervalo_tentativas)

        # Fazemos três passagens iniciais para começar com até três inscrições.
        # Se faltarem canais ou houver falha, completamos nas próximas rodadas.
        for _ in range(3):
            preparar_canais(contexto, inscrito, assinaturas)

        while True:
            canais = preparar_canais(contexto, inscrito, assinaturas)
            if not canais:
                esperar(inscrito, intervalo_tentativas)
                continue
            # Escolhemos um canal por lote, mesmo que não estejamos inscritos nele.
            canal = random.choice(canais)
            for numero in range(10):
                inicio = time.monotonic()
                fazer_pedido(contexto, inscrito, "publish", {
                    "username": nome_bot, "channel": canal,
                    "message": random.choice(frases),
                })
                # O tempo do pedido entra na conta: evitamos somar mais 1 segundo.
                # Se ele demorar mais de 1 segundo, não tentamos compensar com rajadas.
                # Mantemos o intervalo também entre o fim de um lote e o próximo.
                esperar(inscrito, max(0, intervalo_mensagens - (time.monotonic() - inicio)))
            receber_publicacoes(inscrito)
    except KeyboardInterrupt:
        print("Bot encerrado.", flush=True)
    finally:
        inscrito.close(linger=0)
        contexto.term()


if __name__ == "__main__":
    main()
