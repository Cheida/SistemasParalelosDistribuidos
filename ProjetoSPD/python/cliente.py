"""Bot que faz login, lista canais e cria um canal com seu nome.
Execute na pasta do projeto: python python/cliente.py
"""
import os
import random
import time
import msgpack
import zmq

# O Compose passa o endereço. Fora do Docker usamos localhost.
endereco_broker = os.getenv("ENDERECO_BROKER", "tcp://localhost:5555")
# No Docker, cada container tem um hostname diferente.
# Fora dele, mantemos um nome simples para testar pelo terminal.
identificador = os.getenv("HOSTNAME")
nome_padrao = f"bot-py-{identificador[:12]}" if identificador else "bot-python-1"
nome_bot = os.getenv("NOME_BOT", nome_padrao)
tempo_resposta_ms = 5000
# Pausa visual da demonstração; use PAUSA_VISUAL=0 para desativar.
pausa_visual = max(0, min(float(os.getenv("PAUSA_VISUAL", "0.6")), 1))
intervalo_tentativas = 3
intervalo_consultas = 0.6
intervalo_operacoes = 0.6


def mostrar_mensagem(acao, mensagem):
    # Exibimos o timestamp transmitido, junto da operação e dos dados.
    if not isinstance(mensagem, dict):
        print(f"[{nome_bot}] {acao} | mensagem inválida", flush=True)
        return

    # Traduzimos os nomes só na tela; o protocolo continua igual.
    nomes = {
        "login": "login",
        "list_channels": "listar canais",
        "create_channel": "criar canal",
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
    if dados == {}:
        partes.append("dados: {}")
    if "status" in mensagem:
        partes.append(f"status: {mensagem['status']}")
    print(f"[{nome_bot}] {acao} | " + " | ".join(partes), flush=True)

    # Para conferir todos os campos na apresentação, ative MENSAGENS_COMPLETAS=1.
    if os.getenv("MENSAGENS_COMPLETAS", "0") == "1":
        print(f"    conteúdo: {mensagem}", flush=True)


def timestamp():
    # Convertendo nanossegundos para milissegundos, como combinamos no protocolo.
    return time.time_ns() // 1_000_000


def fazer_pedido(contexto, operacao, dados):
    # Recriamos o REQ para uma tentativa nova após um possível timeout.
    socket = contexto.socket(zmq.REQ)
    # Ao fechar, não esperamos mensagens pendentes serem enviadas.
    socket.setsockopt(zmq.LINGER, 0)
    # Os limites evitam que o bot fique preso se não houver resposta.
    socket.setsockopt(zmq.SNDTIMEO, tempo_resposta_ms)
    socket.setsockopt(zmq.RCVTIMEO, tempo_resposta_ms)
    socket.connect(endereco_broker)
    # Cada pedido leva a operação, o horário de envio e os dados necessários.
    pedido = {
        "operation": operacao,
        "timestamp": timestamp(),
        "payload": dados,
    }
    try:
        mostrar_mensagem("ENVIANDO", pedido)
        # Convertemos o dicionário para binário antes de enviar.
        socket.send(msgpack.packb(pedido, use_bin_type=True))
        # Esperamos a resposta e recuperamos os textos como strings.
        resposta = msgpack.unpackb(socket.recv(), raw=False)
        # A resposta já chegou; a pausa só separa os registros na tela.
        time.sleep(pausa_visual)
        mostrar_mensagem("RECEBENDO", resposta)
        # Conferimos se a resposta segue o formato e pertence ao nosso pedido.
        if (
            not isinstance(resposta, dict)
            or resposta.get("operation") != operacao
            or type(resposta.get("timestamp")) is not int
            or not 0 <= resposta["timestamp"] <= 2**63 - 1
            or resposta.get("status") not in (
                "ok", "channel_exists", "invalid_name", "invalid_request",
                "unknown_operation", "internal_error", "invalid_message",
            )
            or not isinstance(resposta.get("payload"), dict)
        ):
            print("⚠️ Resposta fora do formato combinado.", flush=True)
            return None
        return resposta
    except zmq.Again:
        print("⚠️ Não chegou resposta dentro do prazo.", flush=True)
        return None
    except (ValueError, TypeError, msgpack.exceptions.UnpackException):
        print("⚠️ Não foi possível ler a resposta.", flush=True)
        return None
    finally:
        socket.close()


def fazer_login(contexto):
    while True:
        resposta = fazer_pedido(contexto, "login", {"username": nome_bot})
        if resposta is not None and resposta["status"] == "ok":
            return
        print("Vou tentar fazer login novamente.", flush=True)
        time.sleep(intervalo_tentativas)


def escolher_canal(canais, tentar_existente, ultimo_numero):
    # Em uma rodada tentamos um nome novo; na outra, um nome já listado.
    if tentar_existente and canais:
        return random.choice(canais), ultimo_numero

    # Consideramos apenas nomes no formato canal seguido de números ASCII.
    numeros = [
        int(nome[5:]) for nome in canais
        if nome.startswith("canal")
        and nome[5:]
        and all("0" <= letra <= "9" for letra in nome[5:])
    ]
    # Guardar o último número evita voltar para trás ao consultar outro servidor.
    proximo = max([ultimo_numero, *numeros]) + 1
    return f"canal{proximo}", proximo


def main():
    contexto = zmq.Context()
    tentar_existente = False
    ultimo_numero = 0
    try:
        # Só começamos a consultar os canais depois de um login aceito.
        fazer_login(contexto)
        # Essa pausa é só para ficar mais fácil de acompanhar no terminal.
        time.sleep(intervalo_operacoes)
        while True:
            resposta = fazer_pedido(contexto, "list_channels", {})
            if resposta is not None and resposta["status"] == "ok":
                canais = resposta["payload"].get("channels")
                if not isinstance(canais, list) or not all(isinstance(c, str) for c in canais):
                    print("⚠️ Lista de canais inválida.", flush=True)
                else:
                    canal, ultimo_numero = escolher_canal(
                        canais, tentar_existente, ultimo_numero
                    )
                    time.sleep(intervalo_operacoes)
                    criacao = fazer_pedido(contexto, "create_channel", {
                        "username": nome_bot, "channel": canal,
                    })
                    # Se não houve resposta, mantemos o tipo de tentativa.
                    if criacao is not None:
                        tentar_existente = not tentar_existente
            # Pausa curta entre o fim desta troca e a próxima. O loop segue até Control+C.
            time.sleep(intervalo_consultas)
    except KeyboardInterrupt:
        print("Bot encerrado.", flush=True)
    finally:
        # Liberamos o contexto mesmo quando interrompemos o programa.
        contexto.term()


if __name__ == "__main__":
    main()
