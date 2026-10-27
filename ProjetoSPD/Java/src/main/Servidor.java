package br.fei.spd;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;

public class Servidor {

    private final String nomeServidor;
    private final String enderecoBroker;
    private final String enderecoPublicacao;
    private final String arquivoBanco;

    private Connection banco;

    public Servidor() {

        String hostname = Protocolo.ambiente("HOSTNAME", "local");

        if (hostname.length() > 12) {
            hostname = hostname.substring(0, 12);
        }

        nomeServidor = Protocolo.ambiente(
                "NOME_SERVIDOR",
                "servjava-" + hostname
        );

        enderecoBroker = Protocolo.ambiente(
                "ENDERECO_BROKER",
                "tcp://localhost:5556"
        );

        enderecoPublicacao = Protocolo.ambiente(
                "ENDERECO_PUBLICACAO",
                "tcp://localhost:5557"
        );

        arquivoBanco = Protocolo.ambiente(
                "ARQUIVO_BANCO",
                "dados/" + nomeServidor + ".db"
        );
    }

    /*
     * Abre o banco SQLite e cria as tabelas necessárias.
     */
    private void abrirBanco() throws Exception {

        Path caminho = Path.of(arquivoBanco).toAbsolutePath();

        Path diretorio = caminho.getParent();

        if (diretorio != null) {
            Files.createDirectories(diretorio);
        }

        banco = DriverManager.getConnection(
                "jdbc:sqlite:" + caminho
        );

        try (Statement stmt = banco.createStatement()) {

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS logins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """);

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS canais (
                    nome TEXT PRIMARY KEY,
                    criado_por TEXT NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """);

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS publicacoes (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    canal TEXT NOT NULL,
                    mensagem TEXT NOT NULL,
                    timestamp_pedido INTEGER NOT NULL,
                    timestamp_publicacao INTEGER NOT NULL,
                    evento BLOB NOT NULL
                )
            """);

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS trocas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pedido BLOB NOT NULL,
                    resposta BLOB NOT NULL,
                    timestamp_recebimento INTEGER NOT NULL
                )
            """);
        }

        System.out.println(
                "[" + nomeServidor + "] Banco aberto: " + caminho
        );
    }

    /*
     * Registra o pedido e a resposta completos.
     */
    private void registrarTroca(
            byte[] pedido,
            byte[] resposta,
            long timestampRecebimento
    ) throws SQLException {

        String sql = """
            INSERT INTO trocas
            (pedido, resposta, timestamp_recebimento)
            VALUES (?, ?, ?)
        """;

        try (PreparedStatement stmt = banco.prepareStatement(sql)) {

            stmt.setBytes(1, pedido);
            stmt.setBytes(2, resposta);
            stmt.setLong(3, timestampRecebimento);

            stmt.executeUpdate();
        }
    }

    /*
     * Verifica se um nome de canal já existe no banco local.
     */
    private boolean canalExiste(String canal) throws SQLException {

        String sql = "SELECT 1 FROM canais WHERE nome = ?";

        try (PreparedStatement stmt = banco.prepareStatement(sql)) {

            stmt.setString(1, canal);

            try (ResultSet resultado = stmt.executeQuery()) {
                return resultado.next();
            }
        }
    }

    /*
     * Processa uma requisição recebida do broker.
     */
    private Map<String, Object> tratarPedido(
            Map<String, Object> pedido,
            ZMQ.Socket publicador
    ) throws SQLException, IOException {

        Object operacaoObjeto = pedido.get("operation");

        String operacao = operacaoObjeto instanceof String
                ? (String) operacaoObjeto
                : "";

        /*
         * Validamos os campos obrigatórios do envelope.
         */
        if (!pedido.containsKey("operation")
                || !pedido.containsKey("timestamp")
                || !pedido.containsKey("payload")) {

            return Protocolo.resposta(
                    operacao,
                    "invalid_request",
                    Protocolo.mapa()
            );
        }

        if (!(operacaoObjeto instanceof String)
                || !Protocolo.timestampValido(
                        pedido.get("timestamp"))
                || !(pedido.get("payload") instanceof Map<?, ?>)) {

            return Protocolo.resposta(
                    operacao,
                    "invalid_request",
                    Protocolo.mapa()
            );
        }

        long timestampPedido =
                ((Number) pedido.get("timestamp")).longValue();

        Map<String, Object> dados =
                Protocolo.comoMapa(pedido.get("payload"));

        /*
         * LOGIN, CREATE_CHANNEL e PUBLISH precisam de username.
         */
        String usuario = null;

        if (operacao.equals("login")
                || operacao.equals("create_channel")
                || operacao.equals("publish")) {

            Object usuarioObjeto = dados.get("username");

            if (!(usuarioObjeto instanceof String)) {
                return Protocolo.resposta(
                        operacao,
                        "invalid_request",
                        Protocolo.mapa()
                );
            }

            usuario = (String) usuarioObjeto;

            if (!Protocolo.nomeValido(usuario)) {
                return Protocolo.resposta(
                        operacao,
                        "invalid_name",
                        Protocolo.mapa()
                );
            }
        }

        /*
         * ETAPA 1 — LOGIN
         */
        if (operacao.equals("login")) {

            String sql = """
                INSERT INTO logins (username, timestamp)
                VALUES (?, ?)
            """;

            try (PreparedStatement stmt =
                         banco.prepareStatement(sql)) {

                stmt.setString(1, usuario);
                stmt.setLong(2, timestampPedido);

                stmt.executeUpdate();
            }

            System.out.println(
                    "[" + nomeServidor + "] Login: " + usuario
            );

            return Protocolo.resposta(
                    operacao,
                    "ok",
                    Protocolo.mapa("username", usuario)
            );
        }

        /*
         * ETAPA 1 — CRIAR CANAL
         */
        if (operacao.equals("create_channel")) {

            Object canalObjeto = dados.get("channel");

            if (!(canalObjeto instanceof String)) {
                return Protocolo.resposta(
                        operacao,
                        "invalid_request",
                        Protocolo.mapa()
                );
            }

            String canal = (String) canalObjeto;

            if (!Protocolo.nomeValido(canal)) {
                return Protocolo.resposta(
                        operacao,
                        "invalid_name",
                        Protocolo.mapa()
                );
            }

            String sql = """
                INSERT INTO canais (nome, criado_por, timestamp)
                VALUES (?, ?, ?)
            """;

            try (PreparedStatement stmt =
                         banco.prepareStatement(sql)) {

                stmt.setString(1, canal);
                stmt.setString(2, usuario);
                stmt.setLong(3, timestampPedido);

                stmt.executeUpdate();

            } catch (SQLException e) {

                /*
                 * A chave primária impede canais duplicados.
                 */
                if (canalExiste(canal)) {
                    return Protocolo.resposta(
                            operacao,
                            "channel_exists",
                            Protocolo.mapa()
                    );
                }

                throw e;
            }

            System.out.println(
                    "[" + nomeServidor + "] Canal criado: " + canal
            );

            return Protocolo.resposta(
                    operacao,
                    "ok",
                    Protocolo.mapa("channel", canal)
            );
        }

        /*
         * ETAPA 1 — LISTAR CANAIS
         */
        if (operacao.equals("list_channels")) {

            java.util.List<String> canais =
                    new java.util.ArrayList<>();

            String sql =
                    "SELECT nome FROM canais ORDER BY nome";

            try (Statement stmt = banco.createStatement();
                 ResultSet resultado = stmt.executeQuery(sql)) {

                while (resultado.next()) {
                    canais.add(resultado.getString("nome"));
                }
            }

            return Protocolo.resposta(
                    operacao,
                    "ok",
                    Protocolo.mapa("channels", canais)
            );
        }

        /*
         * ETAPA 2 — PUBLICAR MENSAGEM
         */
        if (operacao.equals("publish")) {

            Object canalObjeto = dados.get("channel");
            Object mensagemObjeto = dados.get("message");

            if (!(canalObjeto instanceof String)
                    || !(mensagemObjeto instanceof String)) {

                return Protocolo.resposta(
                        operacao,
                        "invalid_request",
                        Protocolo.mapa()
                );
            }

            String canal = (String) canalObjeto;
            String mensagem = (String) mensagemObjeto;

            if (!Protocolo.nomeValido(canal)) {
                return Protocolo.resposta(
                        operacao,
                        "invalid_name",
                        Protocolo.mapa()
                );
            }

            if (mensagem.isBlank() || mensagem.length() > 500) {
                return Protocolo.resposta(
                        operacao,
                        "invalid_message",
                        Protocolo.mapa()
                );
            }

            /*
             * Não exigimos que o canal exista no banco local.
             *
             * O broker pode encaminhar o pedido a um servidor
             * diferente daquele que criou o canal.
             */
            String idEvento = UUID.randomUUID().toString();

            long timestampPublicacao = Protocolo.timestamp();

            Map<String, Object> evento = Protocolo.mapa(
                    "operation", "publication",
                    "timestamp", timestampPublicacao,
                    "payload", Protocolo.mapa(
                            "id", idEvento,
                            "username", usuario,
                            "channel", canal,
                            "message", mensagem,
                            "request_timestamp", timestampPedido
                    )
            );

            byte[] eventoBytes = Protocolo.empacotar(evento);

            /*
             * Primeiro persistimos a publicação.
             */
            String sql = """
                INSERT INTO publicacoes
                (id, username, canal, mensagem,
                 timestamp_pedido, timestamp_publicacao, evento)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

            try (PreparedStatement stmt =
                         banco.prepareStatement(sql)) {

                stmt.setString(1, idEvento);
                stmt.setString(2, usuario);
                stmt.setString(3, canal);
                stmt.setString(4, mensagem);
                stmt.setLong(5, timestampPedido);
                stmt.setLong(6, timestampPublicacao);
                stmt.setBytes(7, eventoBytes);

                stmt.executeUpdate();
            }

            /*
             * O tópico também é codificado em MessagePack.
             *
             * O evento possui dois frames:
             * 1. Tópico.
             * 2. Conteúdo do evento.
             */
            byte[] topicoBytes = Protocolo.empacotar(canal);

            publicador.send(topicoBytes, ZMQ.SNDMORE);
            publicador.send(eventoBytes, 0);

            System.out.println(
                    "[" + nomeServidor + "] PUBLICAÇÃO | canal: "
                            + canal
                            + " | usuário: " + usuario
                            + " | mensagem: " + mensagem
            );

            return Protocolo.resposta(
                    operacao,
                    "ok",
                    Protocolo.mapa(
                            "channel", canal,
                            "id", idEvento
                    )
            );
        }

        /*
         * Operação desconhecida.
         */
        return Protocolo.resposta(
                operacao,
                "unknown_operation",
                Protocolo.mapa()
        );
    }

    /*
     * Inicializa os sockets e processa as requisições continuamente.
     */
    public void executar() throws Exception {

        abrirBanco();

        try (ZContext contexto = new ZContext()) {

            ZMQ.Socket servidor =
                    contexto.createSocket(SocketType.REP);

            ZMQ.Socket publicador =
                    contexto.createSocket(SocketType.PUB);

            servidor.setLinger(0);
            publicador.setLinger(0);

            servidor.connect(enderecoBroker);
            publicador.connect(enderecoPublicacao);

            System.out.println(
                    "[" + nomeServidor + "] Servidor iniciado."
            );

            System.out.println(
                    "[" + nomeServidor + "] Broker: "
                            + enderecoBroker
            );

            System.out.println(
                    "[" + nomeServidor + "] Publicador: "
                            + enderecoPublicacao
            );

            try {

                while (!Thread.currentThread().isInterrupted()) {

                    /*
                     * O REP espera uma requisição.
                     */
                    byte[] pedidoBytes = servidor.recv(0);

                    if (pedidoBytes == null) {
                        continue;
                    }

                    long recebidoEm = Protocolo.timestamp();

                    Map<String, Object> pedido;
                    Map<String, Object> resposta;

                    try {

                        pedido = Protocolo.desempacotarMapa(
                                pedidoBytes
                        );

                        resposta = tratarPedido(
                                pedido,
                                publicador
                        );

                    } catch (IOException e) {

                        /*
                         * Não foi possível decodificar a mensagem.
                         */
                        pedido = Protocolo.mapa();

                        resposta = Protocolo.resposta(
                                "",
                                "invalid_message",
                                Protocolo.mapa()
                        );

                    } catch (SQLException e) {

                        System.err.println(
                                "[" + nomeServidor
                                        + "] Erro SQLite: "
                                        + e.getMessage()
                        );

                        resposta = Protocolo.resposta(
                                "",
                                "internal_error",
                                Protocolo.mapa()
                        );
                    }

                    /*
                     * Registra a troca antes de enviar a resposta.
                     */
                    byte[] respostaBytes =
                            Protocolo.empacotar(resposta);

                    try {

                        registrarTroca(
                                pedidoBytes,
                                respostaBytes,
                                recebidoEm
                        );

                    } catch (SQLException e) {

                        System.err.println(
                                "[" + nomeServidor
                                        + "] Falha ao registrar troca: "
                                        + e.getMessage()
                        );

                        resposta = Protocolo.resposta(
                                "",
                                "internal_error",
                                Protocolo.mapa()
                        );

                        respostaBytes =
                                Protocolo.empacotar(resposta);
                    }

                    /*
                     * O socket REP deve responder antes de
                     * receber a próxima requisição.
                     */
                    servidor.send(respostaBytes, 0);
                }

            } finally {

                servidor.close();
                publicador.close();
            }
        }
    }

    public static void main(String[] args) {

        Servidor servidor = new Servidor();

        try {

            servidor.executar();

        } catch (Exception e) {

            System.err.println(
                    "Não foi possível iniciar o servidor Java: "
                            + e.getMessage()
            );

            e.printStackTrace();

            System.exit(1);

        } finally {

            try {

                if (servidor.banco != null) {
                    servidor.banco.close();
                }

            } catch (SQLException e) {
                System.err.println(
                        "Erro ao fechar SQLite: " + e.getMessage()
                );
            }
        }
    }
}
