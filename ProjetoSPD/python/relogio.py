"""Relógios usados por cada processo Python da parte 3.
O relógio físico é ajustado só dentro do programa, sem mudar a hora do Mac.
"""
import time


class Relogio:
    def __init__(self, ajuste_inicial_ms=0):
        self.contador = 0
        self.ajuste_ms = ajuste_inicial_ms

    def timestamp(self):
        # Somamos nossa correção ao horário do sistema, em milissegundos.
        return time.time_ns() // 1_000_000 + self.ajuste_ms

    def receber(self, mensagem):
        if not isinstance(mensagem, dict):
            raise ValueError("A mensagem precisa ser um mapa")
        recebido = mensagem.get("clock")
        # bool também é considerado int no Python; por isso usamos type.
        if type(recebido) is not int or not 0 <= recebido <= 2**63 - 1:
            raise ValueError("Contador lógico ausente ou inválido")
        # Seguimos o enunciado: no recebimento usamos max, sem somar 1.
        self.contador = max(self.contador, recebido)

    def preparar_envio(self, mensagem):
        # Chamamos uma vez por mensagem da aplicação, não por frame do ZeroMQ.
        if self.contador >= 2**63 - 1:
            raise OverflowError("O contador lógico atingiu o limite de 64 bits")
        self.contador += 1
        mensagem["clock"] = self.contador
        mensagem["timestamp"] = self.timestamp()
        return mensagem

    def sincronizar(self, hora_referencia, duracao_segundos):
        if type(hora_referencia) is not int or hora_referencia < 0:
            raise ValueError("Hora de referência inválida")
        # Estimamos metade do tempo de ida e volta como atraso da resposta.
        estimativa_agora = hora_referencia + round(duracao_segundos * 1000 / 2)
        hora_local = time.time_ns() // 1_000_000
        self.ajuste_ms = estimativa_agora - hora_local
        return self.ajuste_ms
