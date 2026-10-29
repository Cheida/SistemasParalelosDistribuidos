import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import java.io.IOException;
import java.util.Map;

/* Cadastro e heartbeat usam uma conexão separada dos pedidos dos bots. */
public class RegistroServidor {
    private final ZContext contexto;
    private final String nome;
    private final String endereco;
    private long rank = 0;
    private long proximaTentativa = 0;

    public RegistroServidor(ZContext contexto, String nome) {
        this.contexto = contexto;
        this.nome = nome;
        this.endereco = Protocolo.ambiente("ENDERECO_REFERENCIA", "tcp://localhost:5559");
    }

    private Map<String, Object> pedir(String operacao, Map<String, Object> dados) {
        ZMQ.Socket socket = contexto.createSocket(SocketType.REQ);
        socket.setLinger(0);
        socket.setSendTimeOut(1000);
        socket.setReceiveTimeOut(1000);
        socket.connect(endereco);
        try {
            Map<String, Object> pedido = Protocolo.mapa("operation", operacao, "payload", dados);
            Protocolo.prepararEnvio(pedido);
            long inicio = System.nanoTime();
            if (!socket.send(Protocolo.empacotar(pedido), 0)) return null;
            byte[] bytes = socket.recv(0);
            long duracao = System.nanoTime() - inicio;
            if (bytes == null) return null;
            Map<String, Object> resposta = Protocolo.desempacotarMapa(bytes);
            Protocolo.receberRelogio(resposta);
            if (!operacao.equals(resposta.get("operation"))
                    || !Protocolo.timestampValido(resposta.get("timestamp"))
                    || !(resposta.get("status") instanceof String)
                    || Protocolo.comoMapa(resposta.get("payload")) == null) return null;
            System.out.println("[" + nome + "] REFERÊNCIA | " + operacao
                    + " | clock: " + resposta.get("clock") + " | status: " + resposta.get("status"));
            if ("heartbeat".equals(operacao) && "ok".equals(resposta.get("status"))) {
                // A hora correta está no timestamp da resposta, como no Python e C.
                long ajuste = Protocolo.sincronizar(((Number) resposta.get("timestamp")).longValue(), duracao);
                System.out.println("[" + nome + "] HEARTBEAT | rank: " + rank
                        + " | ajuste físico: " + ajuste + "ms | clock: " + Protocolo.clockAtual());
            }
            return resposta;
        } catch (IOException | RuntimeException e) {
            System.out.println("[" + nome + "] Referência indisponível: " + e.getMessage());
            return null;
        } finally {
            socket.close();
        }
    }

    public boolean registrar() {
        proximaTentativa = System.nanoTime() + 2_000_000_000L;
        Map<String, Object> resposta = pedir("register_server", Protocolo.mapa(
                "name", nome, "address", Protocolo.ambiente("ENDERECO_SERVIDOR",
                        Protocolo.ambiente("HOSTNAME", "localhost"))));
        if (resposta == null || !"ok".equals(resposta.get("status"))) return false;
        Object valor = Protocolo.comoMapa(resposta.get("payload")).get("rank");
        if (!Protocolo.timestampValido(valor) || ((Number) valor).longValue() < 1) return false;
        rank = ((Number) valor).longValue();
        System.out.println("[" + nome + "] REGISTRADO | rank: " + rank);
        return true;
    }

    public void manterCadastro() {
        if (rank == 0 && System.nanoTime() >= proximaTentativa) registrar();
    }

    public void listar() {
        Map<String, Object> resposta = pedir("list_servers", Protocolo.mapa());
        if (resposta != null && "ok".equals(resposta.get("status")))
            System.out.println("[" + nome + "] SERVIDORES | " + resposta.get("payload"));
    }

    public void heartbeat() {
        if (rank == 0 && !registrar()) return;
        Map<String, Object> resposta = pedir("heartbeat", Protocolo.mapa("name", nome));
        if (resposta != null && "server_not_registered".equals(resposta.get("status"))) {
            rank = 0;
            registrar();
        }
    }
}
