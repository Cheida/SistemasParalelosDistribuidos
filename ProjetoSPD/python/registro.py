"""Comunicação do servidor com a referência, sem passar pelo broker dos bots."""
import time
import msgpack
import zmq


class Registro:
    def __init__(self, contexto, relogio, nome, endereco_servidor, endereco_referencia):
        self.contexto = contexto
        self.relogio = relogio
        self.nome = nome
        self.endereco_servidor = endereco_servidor
        self.endereco_referencia = endereco_referencia
        self.rank = None
        self.proxima_tentativa = 0

    def pedir(self, operacao, dados):
        socket = self.contexto.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, 1000)
        socket.connect(self.endereco_referencia)
        pedido = {"operation": operacao, "payload": dados}
        try:
            self.relogio.preparar_envio(pedido)
            print(f"[{self.nome}] ENVIANDO REFERÊNCIA | operação: {operacao} | "
                  f"timestamp: {pedido['timestamp']} | clock: {pedido['clock']}", flush=True)
            inicio = time.monotonic()
            socket.send(msgpack.packb(pedido, use_bin_type=True))
            if not socket.poll(1000):
                print(f"[{self.nome}] Referência sem resposta; tentaremos novamente.", flush=True)
                return None
            resposta = msgpack.unpackb(socket.recv(), raw=False)
            duracao = time.monotonic() - inicio
            self.relogio.receber(resposta)
            if (resposta.get("operation") != operacao
                    or not isinstance(resposta.get("payload"), dict)
                    or not isinstance(resposta.get("status"), str)
                    or type(resposta.get("timestamp")) is not int
                    or not 0 <= resposta["timestamp"] <= 2**63 - 1):
                raise ValueError("Resposta inválida da referência")
            print(f"[{self.nome}] REFERÊNCIA | operação: {operacao} | "
                  f"clock: {resposta['clock']} | status: {resposta['status']}", flush=True)
            return resposta, duracao
        except (zmq.Again, ValueError, TypeError, msgpack.exceptions.UnpackException):
            print(f"[{self.nome}] Não foi possível consultar a referência.", flush=True)
            return None
        finally:
            socket.close()

    def registrar(self):
        self.proxima_tentativa = time.monotonic() + 2
        resultado = self.pedir("register_server", {
            "name": self.nome, "address": self.endereco_servidor,
        })
        if resultado is None:
            return False
        resposta, _ = resultado
        rank = resposta["payload"].get("rank")
        if resposta["status"] != "ok" or type(rank) is not int or rank < 1:
            return False
        self.rank = rank
        print(f"[{self.nome}] REGISTRADO | rank: {self.rank}", flush=True)
        return True

    def listar(self):
        resultado = self.pedir("list_servers", {})
        if resultado is not None and resultado[0]["status"] == "ok":
            print(f"[{self.nome}] SERVIDORES | {resultado[0]['payload'].get('servers', [])}", flush=True)

    def heartbeat(self):
        if self.rank is None and not self.registrar():
            return
        resultado = self.pedir("heartbeat", {"name": self.nome})
        if resultado is None:
            return
        resposta, duracao = resultado
        if resposta["status"] == "server_not_registered":
            self.rank = None
            self.registrar()
        elif resposta["status"] == "ok":
            # A referência manda a hora correta no timestamp da resposta.
            ajuste = self.relogio.sincronizar(resposta["timestamp"], duracao)
            print(f"[{self.nome}] HEARTBEAT | rank: {self.rank} | "
                  f"ajuste físico: {ajuste}ms | clock local: {self.relogio.contador}", flush=True)
