"""Proxy da parte 2. Recebe publicações e distribui para os inscritos."""
import os
import zmq

endereco_publicadores = os.getenv("ENDERECO_PUBLICADORES", "tcp://*:5557")
endereco_inscritos = os.getenv("ENDERECO_INSCRITOS", "tcp://*:5558")


def main():
    contexto = zmq.Context()
    entrada = contexto.socket(zmq.XSUB)
    saida = contexto.socket(zmq.XPUB)
    try:
        # Servidores publicam no XSUB; os bots se inscrevem pelo XPUB.
        entrada.bind(endereco_publicadores)
        saida.bind(endereco_inscritos)
        print("Proxy Pub/Sub iniciado: XSUB 5557 | XPUB 5558", flush=True)
        # O proxy também encaminha as inscrições para os publicadores.
        zmq.proxy(entrada, saida)
    except KeyboardInterrupt:
        print("Proxy encerrado.", flush=True)
    finally:
        entrada.close(linger=0)
        saida.close(linger=0)
        contexto.term()


if __name__ == "__main__":
    main()
