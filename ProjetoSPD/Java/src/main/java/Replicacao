import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.sql.*;
import java.util.*;


public class Replicacao implements AutoCloseable {
    private static final int LOTE = 100;
    private static final Map<String, List<String>> COLUNAS = new LinkedHashMap<>();
    static {
        COLUNAS.put("logins", List.of("id", "username", "timestamp"));
        COLUNAS.put("canais", List.of("nome", "criado_por", "timestamp"));
        COLUNAS.put("publicacoes", List.of("id", "username", "canal", "mensagem",
                "timestamp_pedido", "timestamp_publicacao", "evento"));
        COLUNAS.put("trocas", List.of("id", "pedido", "resposta", "timestamp_recebimento"));
    }
    private final String arquivo, nome, referencia, bind;
    private final long intervalo;
    private volatile boolean parar;
    private volatile Exception falha;
    private Thread ouvinte, trabalhador;

    public Replicacao(String arquivo, String nome) {
        this.arquivo = arquivo;
        this.nome = nome;
        referencia = Protocolo.ambiente("ENDERECO_REFERENCIA", "tcp://localhost:5559");
        bind = Protocolo.ambiente("ENDERECO_REPLICA_BIND", "tcp://*:5561");
        double segundos = Double.parseDouble(Protocolo.ambiente("INTERVALO_REPLICA", "1"));
        if (!Double.isFinite(segundos) || segundos <= 0)
            throw new IllegalArgumentException("INTERVALO_REPLICA deve ser positivo");
        intervalo = Math.max(1, (long) (segundos * 1000));
    }

    Connection conectar() throws SQLException {
        Connection banco = DriverManager.getConnection("jdbc:sqlite:" + arquivo);
        try (Statement s = banco.createStatement()) { s.execute("PRAGMA busy_timeout=5000"); }
        return banco;
    }

    static int executar(Connection b, String sql, Object... valores) throws SQLException {
        try (PreparedStatement s = b.prepareStatement(sql)) {
            for (int i = 0; i < valores.length; i++) s.setObject(i + 1, valores[i]);
            return s.executeUpdate();
        }
    }

    static List<Map<String, Object>> consultar(Connection b, String sql, Object... valores)
            throws SQLException {
        try (PreparedStatement s = b.prepareStatement(sql)) {
            for (int i = 0; i < valores.length; i++) s.setObject(i + 1, valores[i]);
            try (ResultSet r = s.executeQuery()) {
                List<Map<String, Object>> linhas = new ArrayList<>();
                while (r.next()) {
                    Map<String, Object> linha = new LinkedHashMap<>();
                    for (int i = 1; i <= r.getMetaData().getColumnCount(); i++)
                        linha.put(r.getMetaData().getColumnLabel(i), r.getObject(i));
                    linhas.add(linha);
                }
                return linhas;
            }
        }
    }

    @FunctionalInterface interface Transacao { void executar() throws Exception; }
    static void transacao(Connection b, Transacao acao) throws Exception {
        // Reserva a escrita antes de ler, evitando promoção de transação bloqueada.
        executar(b, "BEGIN IMMEDIATE");
        try { acao.executar(); executar(b, "COMMIT"); }
        catch (Exception e) {
            try { executar(b, "ROLLBACK"); } catch (SQLException rollback) { e.addSuppressed(rollback); }
            throw e;
        }
    }

    void preparar(Connection b) throws Exception {
        executar(b, "CREATE TABLE IF NOT EXISTS replica_eventos (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                + "origem TEXT NOT NULL, tipo TEXT NOT NULL, chave TEXT NOT NULL, dados BLOB NOT NULL, "
                + "UNIQUE(origem,tipo,chave))");
        executar(b, "CREATE INDEX IF NOT EXISTS idx_replica_evento_chave ON replica_eventos(tipo,chave)");
        for (String tabela : List.of("replica_capturados", "replica_importados"))
            executar(b, "CREATE TABLE IF NOT EXISTS " + tabela
                    + " (tipo TEXT NOT NULL, chave_local TEXT NOT NULL, PRIMARY KEY(tipo,chave_local))");
        executar(b, "CREATE TABLE IF NOT EXISTS replica_cursores (servidor TEXT PRIMARY KEY, seq INTEGER NOT NULL)");
        executar(b, "CREATE TABLE IF NOT EXISTS replica_vencedor_canal "
                + "(nome TEXT PRIMARY KEY, timestamp INTEGER NOT NULL, origem TEXT NOT NULL)");
        transacao(b, () -> {
            for (var entrada : COLUNAS.entrySet()) {
                String tipo = entrada.getKey();
                for (var linha : consultar(b, "SELECT " + String.join(",", entrada.getValue()) + " FROM " + tipo)) {
                    String chave = linha.get(entrada.getValue().get(0)).toString();
                    if (!consultar(b, "SELECT 1 FROM replica_eventos WHERE tipo=? AND chave=? AND dados=? LIMIT 1",
                            tipo, chave, Protocolo.empacotar(linha)).isEmpty())
                        executar(b, "INSERT OR IGNORE INTO replica_capturados VALUES (?,?)", tipo, chave);
                }
            }
        });
    }

    void capturarLocais(Connection b) throws Exception {
        for (var entrada : COLUNAS.entrySet()) {
            String tipo = entrada.getKey(), chave = entrada.getValue().get(0);
            transacao(b, () -> {
                var linhas = consultar(b, "SELECT " + String.join(",", entrada.getValue()) + " FROM " + tipo
                        + " AS t WHERE NOT EXISTS (SELECT 1 FROM replica_importados i WHERE i.tipo=? "
                        + "AND i.chave_local=CAST(t." + chave + " AS TEXT)) "
                        + "AND NOT EXISTS (SELECT 1 FROM replica_capturados c WHERE c.tipo=? "
                        + "AND c.chave_local=CAST(t." + chave + " AS TEXT)) "
                        + "AND NOT EXISTS (SELECT 1 FROM replica_eventos e WHERE e.origem=? AND e.tipo=? "
                        + "AND e.chave=CAST(t." + chave + " AS TEXT)) LIMIT ?", tipo, tipo, nome, tipo, LOTE);
                for (var linha : linhas) {
                    String id = linha.get(chave).toString();
                    executar(b, "INSERT OR IGNORE INTO replica_eventos(origem,tipo,chave,dados) VALUES (?,?,?,?)",
                            nome, tipo, id, Protocolo.empacotar(linha));
                    executar(b, "INSERT OR IGNORE INTO replica_capturados VALUES (?,?)", tipo, id);
                    if (tipo.equals("canais"))
                        executar(b, "INSERT OR IGNORE INTO replica_vencedor_canal VALUES (?,?,?)",
                                id, linha.get("timestamp"), nome);
                }
            });
        }
    }

    boolean importar(Connection b, Map<String, Object> evento) throws Exception {
        if (evento == null || !(evento.get("origem") instanceof String origem)
                || !(evento.get("tipo") instanceof String tipo) || !COLUNAS.containsKey(tipo)
                || !(evento.get("chave") instanceof String chave)
                || !(evento.get("dados") instanceof byte[] bruto))
            throw new IllegalArgumentException("Evento de réplica inválido");
        var dados = Protocolo.desempacotarMapa(bruto);
        var colunas = COLUNAS.get(tipo);
        if (dados == null || !dados.keySet().equals(new HashSet<>(colunas))
                || !chave.equals(String.valueOf(dados.get(colunas.get(0)))))
            throw new IllegalArgumentException("Dados de réplica inválidos");
        if (executar(b, "INSERT OR IGNORE INTO replica_eventos(origem,tipo,chave,dados) VALUES (?,?,?,?)",
                origem, tipo, chave, bruto) == 0) return false;
        if (tipo.equals("canais")) {
            var anterior = consultar(b, "SELECT timestamp,origem FROM replica_vencedor_canal WHERE nome=?", chave);
            long tempo = ((Number) dados.get("timestamp")).longValue();
            boolean vence = anterior.isEmpty();
            if (!vence) {
                var vencedor = anterior.get(0);
                long tempoAnterior = ((Number) vencedor.get("timestamp")).longValue();
                vence = tempo < tempoAnterior || (tempo == tempoAnterior
                        && compararOrigem(origem, (String) vencedor.get("origem")) < 0);
            }
            if (vence) {
                executar(b, "INSERT OR REPLACE INTO canais VALUES (?,?,?)",
                        dados.get("nome"), dados.get("criado_por"), tempo);
                executar(b, "INSERT OR REPLACE INTO replica_vencedor_canal VALUES (?,?,?)", chave, tempo, origem);
                executar(b, "INSERT OR IGNORE INTO replica_importados VALUES (?,?)", tipo, chave);
            }
        } else if (tipo.equals("publicacoes")) {
            if (inserir(b, tipo, colunas, dados, true) > 0)
                executar(b, "INSERT OR IGNORE INTO replica_importados VALUES (?,?)", tipo, chave);
        } else {
            var campos = colunas.subList(1, colunas.size());
            String filtro = String.join(" AND ", campos.stream().map(c -> c + "=?").toList());
            Object[] valores = campos.stream().map(dados::get).toArray();
            if (consultar(b, "SELECT id FROM " + tipo + " WHERE " + filtro + " LIMIT 1", valores).isEmpty()) {
                inserir(b, tipo, campos, dados, false);
                Object id = consultar(b, "SELECT last_insert_rowid() AS id").get(0).get("id");
                executar(b, "INSERT OR IGNORE INTO replica_importados VALUES (?,?)", tipo, id.toString());
            }
        }
        return true;
    }

    private static int compararOrigem(String a, String b) {
        return Arrays.compare(a.codePoints().toArray(), b.codePoints().toArray());
    }
    private static int inserir(Connection b, String tipo, List<String> campos,
                               Map<String, Object> dados, boolean ignorar) throws SQLException {
        return executar(b, "INSERT " + (ignorar ? "OR IGNORE " : "") + "INTO " + tipo
                + " (" + String.join(",", campos) + ") VALUES ("
                + String.join(",", Collections.nCopies(campos.size(), "?")) + ")",
                campos.stream().map(dados::get).toArray());
    }

    private Map<String, Object> requisitar(ZContext contexto, String destino, Map<String, Object> pedido) {
        ZMQ.Socket socket = contexto.createSocket(SocketType.REQ);
        try {
            socket.setLinger(0);
            socket.setSendTimeOut(350);
            socket.setReceiveTimeOut(350);
            socket.connect(destino);
            if (!socket.send(Protocolo.empacotar(pedido), 0)) return null;
            byte[] resposta = socket.recv(0);
            return resposta == null ? null : Protocolo.desempacotarMapa(resposta);
        } catch (Exception e) { return null; }
        finally { contexto.destroySocket(socket); }
    }

    private Map<String, Object> pedidoReferencia(String operacao, Map<String, Object> dados) {
        var pedido = Protocolo.mapa("operation", operacao, "payload", dados);
        Protocolo.prepararEnvio(pedido);
        return pedido;
    }

    private void sincronizar(ZContext contexto, Connection b) throws Exception {
        var resposta = requisitar(contexto, referencia, pedidoReferencia("list_servers", Protocolo.mapa()));
        if (resposta == null || !"ok".equals(resposta.get("status"))) return;
        var payload = Protocolo.comoMapa(resposta.get("payload"));
        if (payload == null || !(payload.get("servers") instanceof List<?> pares)) return;
        for (Object objeto : pares) {
            if (parar) return;
            var par = Protocolo.comoMapa(objeto);
            if (par == null || !(par.get("name") instanceof String outro) || nome.equals(outro)
                    || !(par.get("address") instanceof String endereco) || !endereco.startsWith("tcp://")) continue;
            String destino = endereco.substring(0, endereco.lastIndexOf(':')) + ":5561";
            var cursores = consultar(b, "SELECT seq FROM replica_cursores WHERE servidor=?", outro);
            long desde = cursores.isEmpty() ? 0 : ((Number) cursores.get(0).get("seq")).longValue();
            for (int tentativa = 0; tentativa < 10 && !parar; tentativa++) {
                var lote = requisitar(contexto, destino, Protocolo.mapa("desde", desde));
                if (lote == null || !(lote.get("eventos") instanceof List<?> eventos) || eventos.isEmpty()) break;
                final long inicio = desde;
                long[] ultimo = {desde};
                transacao(b, () -> {
                    for (Object item : eventos) {
                        var evento = Protocolo.comoMapa(item);
                        if (evento == null || !Protocolo.timestampValido(evento.get("seq"))
                                || ((Number) evento.get("seq")).longValue() <= ultimo[0])
                            throw new IllegalArgumentException("Sequência de réplica inválida após " + inicio);
                        importar(b, evento);
                        ultimo[0] = ((Number) evento.get("seq")).longValue();
                    }
                    executar(b, "INSERT OR REPLACE INTO replica_cursores VALUES (?,?)", outro, ultimo[0]);
                });
                desde = ultimo[0];
                System.out.println("[" + nome + "] RÉPLICA | " + eventos.size() + " eventos recebidos de " + outro);
                if (eventos.size() < LOTE) break;
            }
        }
    }

    public void iniciar() throws Exception {
        try (Connection b = conectar()) { preparar(b); }
        java.util.concurrent.CompletableFuture<Void> pronto = new java.util.concurrent.CompletableFuture<>();
        ouvinte = new Thread(() -> {
            try (ZContext contexto = new ZContext(); Connection b = conectar()) {
                ZMQ.Socket socket = contexto.createSocket(SocketType.REP);
                socket.setLinger(0);
                socket.setReceiveTimeOut(200);
                socket.setSendTimeOut(350);
                if (!socket.bind(bind)) throw new IllegalStateException("Falha no bind " + bind);
                pronto.complete(null);
                System.out.println("[" + nome + "] RÉPLICA | ouvindo em " + bind);
                while (!parar) {
                    byte[] bytes = socket.recv(0);
                    if (bytes == null) continue;
                    Map<String, Object> resposta;
                    try {
                        var pedido = Protocolo.desempacotarMapa(bytes);
                        Object valor = pedido == null ? null : pedido.get("desde");
                        long desde = Protocolo.timestampValido(valor) ? ((Number) valor).longValue() : 0;
                        resposta = Protocolo.mapa("eventos", consultar(b,
                                "SELECT seq,origem,tipo,chave,dados FROM replica_eventos WHERE seq>? ORDER BY seq LIMIT ?",
                                desde, LOTE));
                    } catch (Exception e) {
                        resposta = Protocolo.mapa("eventos", List.of(), "erro", "Pedido de réplica inválido");
                    }
                    socket.send(Protocolo.empacotar(resposta), 0);
                }
            } catch (Exception e) { falha = e; pronto.completeExceptionally(e); }
        }, "replica-ouvinte");
        ouvinte.start();
        pronto.get();
        trabalhador = new Thread(() -> {
            try (ZContext contexto = new ZContext(); Connection b = conectar()) {
                long heartbeatEm = 0;
                while (!parar) {
                    try {
                        if (System.nanoTime() >= heartbeatEm) {
                            requisitar(contexto, referencia, pedidoReferencia("heartbeat", Protocolo.mapa("name", nome)));
                            heartbeatEm = System.nanoTime() + 20_000_000_000L;
                        }
                        capturarLocais(b);
                        sincronizar(contexto, b);
                    } catch (Exception e) { System.err.println("[" + nome + "] RÉPLICA | erro: " + e.getMessage()); }
                    Thread.sleep(intervalo);
                }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            catch (Exception e) { falha = e; }
        }, "replica-sincronizacao");
        trabalhador.start();
    }

    public void verificar() {
        if (falha != null) throw new IllegalStateException("Falha na replicação", falha);
    }

    @Override public void close() {
        parar = true;
        if (trabalhador != null) trabalhador.interrupt();
        boolean interrompido = false;
        for (Thread t : new Thread[]{ouvinte, trabalhador}) {
            if (t == null) continue;
            while (t.isAlive()) {
                try { t.join(); }
                catch (InterruptedException e) { interrompido = true; }
            }
        }
        if (interrompido) Thread.currentThread().interrupt();
    }
}
