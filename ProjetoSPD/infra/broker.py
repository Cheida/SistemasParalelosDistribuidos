"""Encaminha mensagens entre bots e servidores. Execute: python infra/broker.py"""
import os
import zmq

# O asterisco permite receber conexões pela rede dos containers.
endereco_clientes = os.getenv("ENDERECO_CLIENTES", "tcp://*:5555")
endereco_servidores = os.getenv("ENDERECO_SERVIDORES", "tcp://*:5556")


def main():
    contexto = zmq.Context()
    # ROUTER mantém a identificação das conexões dos clientes.
    entrada = contexto.socket(zmq.ROUTER)
    # DEALER encaminha os pedidos para os servidores conectados.
    saida = contexto.socket(zmq.DEALER)
    try:
        # O broker abre as portas; os outros programas se conectam nelas.
        entrada.bind(endereco_clientes)
        saida.bind(endereco_servidores)
        print(" Broker iniciado.", flush=True)
        # O proxy mantém os frames de roteamento e não abre o MessagePack.
        zmq.proxy(entrada, saida)
    except KeyboardInterrupt:
        print("Broker encerrado.", flush=True)
    finally:
        # Fechamos os sockets sem esperar mensagens pendentes.
        entrada.close(linger=0)
        saida.close(linger=0)
        contexto.term()


if __name__ == "__main__":
    main()
