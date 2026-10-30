"""Eleição e sincronização de Berkeley dos servidores da parte 4.
Os sockets de controle ficam separados dos sockets usados pelos bots.
"""
import os
import queue
import threading
import time
import uuid
import msgpack
import zmq


def calcular_correcoes(diferencas):
    # A diferença do coordenador também entra na média, com valor zero.
    media = sum(diferencas.values()) / len(diferencas)
    return media, {nome: round(media - valor) for nome, valor in diferencas.items()}


class Coordenacao:
    def __init__(self, contexto, relogio, registro, endereco_bind, endereco_pub, endereco_sub):
        self.contexto = contexto
        self.relogio = relogio
        self.registro = registro
        self.nome = registro.nome
        self.endereco_bind = endereco_bind
        self.endereco_pub = endereco_pub
        self.endereco_sub = endereco_sub
        self.trava = threading.RLock()
        self.parar = threading.Event()
        self.eleicao = threading.Event()
        self.sincronizacoes_pendentes = 0
        self.pedidos_sync = queue.Queue()
        self.pronto = threading.Event()
        self.erro = None
        self.coordenador = None
        self.dados_coordenador = None
        self.servidores = {}
        self.mensagens = 0
        self.rodadas = 0
        self.rodada_aplicada = None
        self.historico_rodadas = set()
        self.aguardar_vitoria = 0
        self.threads = []

    def iniciar(self):
        for destino in (self._ouvir, self._trabalhar):
            thread = threading.Thread(target=destino, daemon=True)
            self.threads.append(thread)
            thread.start()
        if not self.pronto.wait(3) or self.erro:
            self.fechar()
            raise RuntimeError(f"Não foi possível abrir o controle do servidor: {self.erro}")

    def fechar(self):
        self.parar.set()
        for thread in self.threads:
            thread.join()

    def contar_mensagem(self):
        # Contamos REQ recebido, REP enviado e PUB de canal enviado.
        # O tráfego do próprio sincronizador não dispara outra rodada recursiva.
        with self.trava:
            self.mensagens += 1
            if self.mensagens >= 15:
                self.mensagens -= 15
                self.sincronizacoes_pendentes += 1

    def _destacar(self, titulo, detalhe):
        # Um bloco único destaca a eleição sem atrasar a rede com sleeps.
        linha = "=" * 60
        print(f"\n{linha}\n[{self.nome}] {titulo}\n{detalhe}\n{linha}\n", flush=True)

    def _eu(self):
        return {"name": self.nome, "rank": self.registro.rank,
                "address": self.registro.endereco_servidor}

    def _lider(self):
        with self.trava:
            return dict(self.dados_coordenador) if self.dados_coordenador else None

    def _aceitar(self, candidato):
        if (not isinstance(candidato, dict)
                or not isinstance(candidato.get("name"), str)
                or type(candidato.get("rank")) is not int
                or not isinstance(candidato.get("address"), str)
                or not candidato["address"].startswith("tcp://")
                or self.registro.rank is None):
            return False
        if candidato["rank"] < self.registro.rank:
            self.eleicao.set()
            return False
        with self.trava:
            atual = self.dados_coordenador
            if atual and candidato["rank"] < atual["rank"]:
                return False
            mudou = self.coordenador != candidato["name"]
            self.dados_coordenador = dict(candidato)
            self.coordenador = candidato["name"]
            self.aguardar_vitoria = 0
        if mudou:
            self._destacar("COORDENADOR DEFINIDO",
                           f"nome: {self.coordenador} | rank: {candidato['rank']}")
        return True

    def _rpc(self, endereco, operacao, dados, timeout=0.7):
        # Cada thread usa seus próprios sockets. Nunca passamos um socket para outra.
        socket = self.contexto.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(endereco)
        pedido = {"operation": operacao, "payload": dados}
        inicio = time.monotonic()
        try:
            with self.relogio.trava:
                self.relogio.preparar_envio(pedido)
                socket.send(msgpack.packb(pedido, use_bin_type=True), flags=zmq.DONTWAIT)
            while not self.parar.is_set() and time.monotonic() - inicio < timeout:
                if socket.poll(50):
                    resposta = msgpack.unpackb(socket.recv(), raw=False)
                    duracao = time.monotonic() - inicio
                    self.relogio.receber(resposta)
                    if (resposta.get("operation") != operacao
                            or type(resposta.get("timestamp")) is not int
                            or not isinstance(resposta.get("payload"), dict)
                            or not isinstance(resposta.get("status"), str)):
                        return None
                    return resposta, duracao
        except (zmq.ZMQError, ValueError, TypeError, msgpack.exceptions.UnpackException):
            pass
        finally:
            socket.close()
        return None

    def _status(self):
        lider = self._lider()
        with self.trava:
            return {"name": self.nome, "rank": self.registro.rank,
                    "coordinator": lider["name"] if lider else None,
                    "time_ms": self.relogio.timestamp(),
                    "adjustment_ms": self.relogio.ajuste_atual(),
                    "rounds": self.rodadas, "last_round": self.rodada_aplicada,
                    "messages_since_sync": self.mensagens}

    def _atender(self, pedido):
        operacao = pedido.get("operation")
        dados = pedido.get("payload")
        if (not isinstance(operacao, str) or not isinstance(dados, dict)
                or type(pedido.get("timestamp")) is not int):
            return "invalid_request", {}
        if operacao == "election":
            if type(dados.get("rank")) is not int:
                return "invalid_request", {}
            # Respondemos logo. A eleição ocorre na outra thread, sem prender o REP.
            if self.registro.rank is not None and self.registro.rank > dados["rank"]:
                self.eleicao.set()
            return "ok", self._eu()
        if operacao == "coordinator":
            return ("ok" if self._aceitar(dados) else "lower_rank"), {}
        if operacao in ("server_status", "clock_read"):
            return "ok", self._status()
        if operacao == "coordinator_status":
            return ("ok" if self.coordenador == self.nome else "not_coordinator"), self._status()
        if operacao == "clock_adjust":
            lider = self._lider()
            rodada = dados.get("round_id")
            if (not lider or dados.get("coordinator") != lider["name"]
                    or dados.get("rank") != lider["rank"]):
                return "not_coordinator", {}
            if not isinstance(rodada, str) or type(dados.get("adjustment_ms")) is not int:
                return "invalid_request", {}
            with self.trava:
                # Uma retransmissão da mesma rodada não pode aplicar o ajuste duas vezes.
                if rodada not in self.historico_rodadas:
                    self.relogio.aplicar_ajuste(dados["adjustment_ms"])
                    self.historico_rodadas.add(rodada)
                    self.rodada_aplicada = rodada
                    print(f"[{self.nome}] BERKELEY | correção: {dados['adjustment_ms']}ms | "
                          f"coordenador: {lider['name']} | rodada: {rodada}", flush=True)
            return "ok", {"round_id": rodada}
        if operacao == "clock_sync":
            if self.coordenador != self.nome:
                return "not_coordinator", self._status()
            if not isinstance(dados.get("name"), str):
                return "invalid_request", {}
            # A thread de trabalho coleta horários. A thread de quem pediu continua
            # atendendo clock_read e clock_adjust enquanto aguarda esta resposta.
            concluido = threading.Event()
            resultado = {}
            self.pedidos_sync.put((dados["name"], concluido, resultado))
            limite = time.monotonic() + 15
            while not self.parar.is_set() and time.monotonic() < limite:
                if concluido.wait(0.05):
                    return resultado["status"], resultado["payload"]
            return "sync_timeout", {}
        return "unknown_operation", {}

    def _ouvir(self):
        rep = self.contexto.socket(zmq.REP)
        sub = self.contexto.socket(zmq.SUB)
        rep.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.LINGER, 0)
        try:
            rep.bind(self.endereco_bind)
            sub.connect(self.endereco_sub)
            sub.setsockopt(zmq.SUBSCRIBE, msgpack.packb("servers", use_bin_type=True))
            self.pronto.set()
            while not self.parar.is_set():
                if sub.poll(0):
                    try:
                        frames = sub.recv_multipart()
                        if len(frames) != 2 or msgpack.unpackb(frames[0], raw=False) != "servers":
                            raise ValueError("Tópico inválido")
                        evento = msgpack.unpackb(frames[1], raw=False)
                        self.relogio.receber(evento)
                        if evento.get("operation") == "coordinator":
                            self._aceitar(evento.get("payload"))
                    except (ValueError, TypeError, msgpack.exceptions.UnpackException):
                        print(f"[{self.nome}] Anúncio de coordenador inválido.", flush=True)
                if not rep.poll(30):
                    continue
                pedido = {}
                try:
                    pedido = msgpack.unpackb(rep.recv(), raw=False)
                    self.relogio.receber(pedido)
                    status, dados = self._atender(pedido)
                except (ValueError, TypeError, KeyError, msgpack.exceptions.UnpackException):
                    status, dados = "invalid_request", {}
                resposta = {"operation": pedido.get("operation", "") if isinstance(pedido, dict) else "",
                            "status": status, "payload": dados}
                with self.relogio.trava:
                    self.relogio.preparar_envio(resposta)
                    rep.send(msgpack.packb(resposta, use_bin_type=True))
        except Exception as erro:
            self.erro = str(erro)
            self.pronto.set()
            self.parar.set()
            print(f"[{self.nome}] Erro no controle: {erro}", flush=True)
        finally:
            rep.close()
            sub.close()

    def _descobrir(self):
        resultado = self._rpc(self.registro.endereco_referencia, "list_servers", {}, timeout=1)
        if resultado is None or resultado[0]["status"] != "ok":
            return
        lista = resultado[0]["payload"].get("servers", [])
        validos = {}
        for servidor in lista:
            if (isinstance(servidor, dict) and isinstance(servidor.get("name"), str)
                    and type(servidor.get("rank")) is int
                    and isinstance(servidor.get("address"), str)
                    and servidor["address"].startswith("tcp://")):
                validos[servidor["name"]] = servidor
        with self.trava:
            novos = set(validos) - set(self.servidores)
            self.servidores = validos
        # Um servidor de rank maior que acaba de entrar deve poder assumir.
        lider = self._lider()
        if lider and any(validos[n]["rank"] > lider["rank"] for n in novos):
            self.eleicao.set()

    def _anunciar(self, pub, confirmar=False):
        with self.relogio.trava:
            evento = self.relogio.preparar_envio({"operation": "coordinator", "payload": self._eu()})
            pub.send_multipart([msgpack.packb("servers", use_bin_type=True),
                                msgpack.packb(evento, use_bin_type=True)])
        print(f"[{self.nome}] PUB servers | coordenador: {self.nome} | "
              f"rank: {self.registro.rank} | clock: {evento['clock']}", flush=True)
        if confirmar:
            for nome, servidor in list(self.servidores.items()):
                if nome != self.nome:
                    self._rpc(servidor["address"], "coordinator", self._eu())

    def _eleger(self, pub):
        self.eleicao.clear()
        with self.trava:
            self.coordenador = None
            self.dados_coordenador = None
        self._destacar("ELEIÇÃO INICIADA", f"Servidor candidato: {self.nome} | rank: {self.registro.rank}")
        superiores = [s for s in self.servidores.values()
                      if s["rank"] > self.registro.rank]
        respondeu = False
        for servidor in superiores:
            resultado = self._rpc(servidor["address"], "election", self._eu())
            if resultado is not None and resultado[0]["status"] == "ok":
                respondeu = True
        if respondeu:
            self.aguardar_vitoria = time.monotonic() + 3
        else:
            if self._aceitar(self._eu()):
                self._anunciar(pub, confirmar=True)

    def _berkeley(self):
        if self.coordenador != self.nome:
            return None
        rodada = str(uuid.uuid4())
        diferencas = {self.nome: 0}
        participantes = {}
        # Coletamos horários e estimamos a metade do RTT de cada resposta.
        for nome, servidor in list(self.servidores.items()):
            if nome == self.nome:
                continue
            resultado = self._rpc(servidor["address"], "clock_read", {})
            if resultado is not None and resultado[0]["status"] == "ok":
                resposta, duracao = resultado
                diferencas[nome] = resposta["timestamp"] + duracao * 500 - self.relogio.timestamp()
                participantes[nome] = servidor
        # Se outra eleição ocorreu durante a coleta, abandonamos esta rodada.
        if self.coordenador != self.nome:
            return None
        media, correcoes = calcular_correcoes(diferencas)
        confirmados = {self.nome}
        for nome, servidor in participantes.items():
            resultado = self._rpc(servidor["address"], "clock_adjust", {
                "coordinator": self.nome, "rank": self.registro.rank,
                "round_id": rodada, "adjustment_ms": correcoes[nome],
            })
            if resultado is not None and resultado[0]["status"] == "ok":
                confirmados.add(nome)
        self.relogio.aplicar_ajuste(correcoes[self.nome])
        with self.trava:
            self.rodadas += 1
            self.rodada_aplicada = rodada
        print(f"[{self.nome}] BERKELEY | média: {media:.1f}ms | correções: {correcoes} | "
              f"confirmados: {sorted(confirmados)}", flush=True)
        return {"round_id": rodada, "confirmed": confirmados}

    def _perder_coordenador(self, lider):
        with self.trava:
            # Uma resposta atrasada não deve derrubar um coordenador mais novo.
            if self.coordenador == lider["name"]:
                self.coordenador = None
                self.dados_coordenador = None
                self.eleicao.set()

    def _sincronizar(self):
        lider = self._lider()
        if not lider:
            self.eleicao.set()
            return
        if lider["name"] == self.nome:
            self._berkeley()
        else:
            resultado = self._rpc(lider["address"], "clock_sync", {"name": self.nome}, timeout=8)
            if resultado is None or resultado[0]["status"] == "not_coordinator":
                self._perder_coordenador(lider)
            else:
                print(f"[{self.nome}] SINCRONIZAÇÃO | coordenador: {lider['name']} | "
                      f"status: {resultado[0]['status']}", flush=True)

    def _trabalhar(self):
        pub = self.contexto.socket(zmq.PUB)
        pub.setsockopt(zmq.LINGER, 0)
        pub.connect(self.endereco_pub)
        descobrir_em = 0
        verificar_em = time.monotonic() + 1
        anunciar_em = 0
        try:
            # Esperamos o cadastro inicial, que é feito pelo servidor principal.
            while not self.parar.is_set():
                if self.registro.rank is None:
                    self.parar.wait(0.05)
                    continue
                agora = time.monotonic()
                if agora >= descobrir_em:
                    self._descobrir()
                    descobrir_em = time.monotonic() + 2
                if self.eleicao.is_set():
                    self._eleger(pub)
                lider = self._lider()
                if not lider and agora >= self.aguardar_vitoria:
                    self._eleger(pub)
                elif lider and lider["name"] == self.nome and agora >= anunciar_em:
                    # Repetimos o PUB para cobrir inscrições tardias e perda do anúncio.
                    self._anunciar(pub)
                    anunciar_em = time.monotonic() + 2
                elif lider and lider["name"] != self.nome and agora >= verificar_em:
                    resultado = self._rpc(lider["address"], "coordinator_status", {}, timeout=3)
                    verificar_em = time.monotonic() + 2
                    if resultado is None or resultado[0]["status"] != "ok":
                        self._destacar("COORDENADOR INDISPONÍVEL",
                                       f"Sem resposta de {lider['name']}. Vamos iniciar uma nova ELEIÇÃO.")
                        self._perder_coordenador(lider)
                try:
                    solicitante, concluido, resultado_sync = self.pedidos_sync.get_nowait()
                except queue.Empty:
                    pass
                else:
                    rodada = self._berkeley()
                    aplicado = rodada is not None and solicitante in rodada["confirmed"]
                    resultado_sync.update(status="ok" if aplicado else "sync_incomplete", payload={
                        "coordinator": self.coordenador,
                        "time_ms": self.relogio.timestamp(),
                        "round_id": rodada["round_id"] if rodada else None,
                        "applied": aplicado,
                    })
                    concluido.set()
                with self.trava:
                    sincronizar = self.sincronizacoes_pendentes > 0 and self.coordenador is not None
                    if sincronizar:
                        self.sincronizacoes_pendentes -= 1
                if sincronizar:
                    self._sincronizar()
                self.parar.wait(0.03)
        except Exception as erro:
            self.erro = str(erro)
            self.parar.set()
            print(f"[{self.nome}] Erro na coordenação: {erro}", flush=True)
        finally:
            pub.close()
