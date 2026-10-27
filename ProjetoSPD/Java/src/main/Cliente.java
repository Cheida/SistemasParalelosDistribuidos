import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Cliente {

    private final String enderecoBroker;
    private final String enderecoInscricao;
    private final String nomeBot;

    private final int tempoRespostaMs = 5000;
    private final int intervaloTentativas = 2;
    private final int intervaloMensagens = 1;

    private final Random random = new Random();

    private final List<String> frases = List.of(
            "Bom dia, pessoal!",
            "Alguém está por aqui?",
            "Teste de publicação.",
            "Mensagem enviada pelo bot.",
            "Tudo certo por aqui!",
            "Olá, canal!"
    );

    public Cliente() {

        String hostname = Protocolo.ambiente(
                "HOSTNAME",
                nomeComputador()
        );

        if (hostname.length() > 12) {
            hostname = hostname.substring(0, 12);
        }

        nomeBot = Protocolo.ambiente(
                "NOME_BOT",
                "bot-java-" + hostname
        );

        enderecoBroker = Protocolo.ambiente(
                "ENDERECO_BROKER",
                "tcp://localhost:5555"
        );

        enderecoInscricao = Protocolo.ambiente(
                "ENDERECO_INSCRICAO",
                "tcp://localhost:5558"
        );
    }

    /*
     * Obtém o hostname do computador.
     */
    private static String nomeComputador() {

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "local";
        }
    }

    /*
     * Exibe os campos principais de uma mensagem.
     */
    private void mostrarMensagem(
            String acao,
            Map<String, Object> mensagem
    ) {

        Map<String, Object> dados =
                Protocolo.comoMapa(mensagem.get("payload"));

        if (dados == null) {
            dados = Protocolo.mapa();
        }

        StringBuilder texto = new StringBuilder();

        texto.append("[")
                .append(nomeBot)
                .append("] ")
                .append(acao)
                .append(" | ")
                .append(mensagem.getOrDefault(
                        "operation", "desconhecida"))
                .append(" | timestamp: ")
                .append(mensagem.getOrDefault(
                        "timestamp", "ausente"));

        if (dados.containsKey("username")) {
            texto.append(" | usuário: ")
                    .append(dados.get("username"));
        }

        if (dados.containsKey("channel")) {
            texto.append(" | canal: ")
                    .append(dados.get("channel"));
        }

        if (dados.containsKey("message")) {
            texto.append(" | mensagem: ")
                    .append(dados.get("message"));
        }

        if (dados.get("channels") instanceof List<?> canais) {

            texto.append(" | canais: ")
                    .append(String.join(
                            ", ",
                            canais.stream()
                                    .map(String::valueOf)
                                    .toList()
                    ));
        }

        if (mensagem.containsKey("status")) {
            texto.append(" | status: ")
                    .append(mensagem.get("status"));
        }

        System.out.println(texto);

        if ("1".equals(System.getenv("MENSAGENS_COMPLETAS"))) {
            System.out.println("    conteúdo: " + mensagem);
        }
    }

    /*
     * Recebe as publicações disponíveis no socket SUB.
     *
     * Cada publicação contém:
     * 1. Tópico em MessagePack.
     * 2. Evento em MessagePack.
     */
    private void receberPublicacoes(ZMQ.Socket inscrito) {

        for (int i = 0; i < 100; i++) {

            byte[] primeiroFrame = inscrito.recv(ZMQ.DONTWAIT);

            if (primeiroFrame == null) {
                return;
            }

            try {

                List<byte[]> frames = new ArrayList<>();

                frames.add(primeiroFrame);

                while (inscrito.hasReceiveMore()) {

                    byte[] frame = inscrito.recv(0);

                    if (frame == null) {
                        throw new IOException(
                                "Frame de publicação inválido."
                        );
                    }

                    frames.add(frame);
                }

                if (frames.size() != 2) {
                    throw new IOException(
                            "A publicação precisa ter dois frames."
                    );
                }

                String canal = Protocolo.MAPPER.readValue(
                        frames.get(0),
                        String.class
                );

                Map<String, Object> evento =
                        Protocolo.desempacotarMapa(
                                frames.get(1)
                        );

                Map<String, Object> dados =
                        Protocolo.comoMapa(
                                evento.get("payload")
                        );

                if (dados == null
                        || !"publication".equals(
                                evento.get("operation"))
                        || !canal.equals(dados.get("channel"))
                        || !(evento.get("timestamp")
                                instanceof Number)
                        || !(dados.get("message") instanceof String)
                        || !(dados.get("username") instanceof String)) {

                    throw new IOException(
                            "Publicação fora do protocolo."
                    );
                }

                long recebidoEm = Protocolo.timestamp();

                System.out.println(
                        "[" + nomeBot + "] PUBLICAÇÃO"
                                + " | canal: " + canal
                                + " | usuário: " + dados.get("username")
                                + " | mensagem: " + dados.get("message")
                                + " | envio: " + evento.get("timestamp")
                                + " | recebimento: " + recebidoEm
                );

                if ("1".equals(
                        System.getenv("MENSAGENS_COMPLETAS"))) {

                    System.out.println("    conteúdo: " + evento);
                }

            } catch (IOException | RuntimeException e) {

                System.out.println(
                        "[" + nomeBot
                                + "] Publicação inválida recebida."
                );
            }
        }
    }

    /*
     * Espera durante determinado intervalo sem parar de receber
     * as publicações dos canais inscritos.
     */
    private void esperar(
            ZMQ.Socket inscrito,
            double segundos
    ) throws InterruptedException {

        long fim = System.nanoTime()
                + (long) (segundos * 1_000_000_000L);

        while (true) {

            receberPublicacoes(inscrito);

            long restante = fim - System.nanoTime();

            if (restante <= 0) {
                return;
            }

            long esperaMs = Math.max(
                    1,
                    Math.min(100, restante / 1_000_000L)
            );

            Thread.sleep(esperaMs);
        }
    }

    /*
     * Envia um pedido REQ e aguarda a resposta.
     *
     * Cada pedido utiliza um socket próprio. Se houver timeout,
     * o socket é fechado e poderá ser criado outro na próxima
     * tentativa.
     */
    private Map<String, Object> fazerPedido(
            ZContext contexto,
            ZMQ.Socket inscrito,
            String operacao,
            Map<String, Object> dados
    ) {

        Map<String, Object> pedido = Protocolo.mapa(
                "operation", operacao,
                "timestamp", Protocolo.timestamp(),
                "payload", dados
        );

        ZMQ.Socket socket = contexto.createSocket(SocketType.REQ);

        socket.setLinger(0);
        socket.setSendTimeOut(tempoRespostaMs);
        socket.connect(enderecoBroker);

        try {

            mostrarMensagem("ENVIANDO", pedido);

            byte[] pedidoBytes = Protocolo.empacotar(pedido);

            if (!socket.send(pedidoBytes, 0)) {

                System.out.println(
                        "[" + nomeBot + "] Falha no envio."
                );

                return null;
            }

            long fim = System.nanoTime()
                    + tempoRespostaMs * 1_000_000L;

            while (System.nanoTime() < fim) {

                /*
                 * Continuamos recebendo publicações enquanto
                 * aguardamos a resposta do servidor.
                 */
                receberPublicacoes(inscrito);

                byte[] respostaBytes =
                        socket.recv(ZMQ.DONTWAIT);

                if (respostaBytes != null) {

                    Map<String, Object> resposta =
                            Protocolo.desempacotarMapa(
                                    respostaBytes
                            );

                    if (!operacao.equals(
                            resposta.get("operation"))
                            || !Protocolo.timestampValido(
                                    resposta.get("timestamp"))
                            || !(resposta.get("status")
                                    instanceof String)
                            || !(resposta.get("payload")
                                    instanceof Map<?, ?>)) {

                        throw new IOException(
                                "Resposta fora do protocolo."
                        );
                    }

                    mostrarMensagem("RECEBENDO", resposta);

                    return resposta;
                }

                Thread.sleep(50);
            }

            System.out.println(
                    "[" + nomeBot + "] Tempo de resposta esgotado."
            );

        } catch (IOException e) {

            System.out.println(
                    "[" + nomeBot
                            + "] Erro de comunicação: "
                            + e.getMessage()
            );

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

        } finally {

            socket.close();
        }

        return null;
    }

    /*
     * Consulta a lista de canais.
     */
    private List<String> listarCanais(
            ZContext contexto,
            ZMQ.Socket inscrito
    ) {

        Map<String, Object> resposta = fazerPedido(
                contexto,
                inscrito,
                "list_channels",
                Protocolo.mapa()
        );

        if (resposta == null
                || !"ok".equals(resposta.get("status"))) {
            return null;
        }

        Map<String, Object> payload =
                Protocolo.comoMapa(resposta.get("payload"));

        if (payload == null
                || !(payload.get("channels") instanceof List<?> lista)) {
            return null;
        }

        List<String> canais = new ArrayList<>();

        for (Object item : lista) {

            if (!(item instanceof String)) {
                return null;
            }

            canais.add((String) item);
        }

        return canais;
    }

    /*
     * Reproduz a preparação de canais do cliente Python.
     *
     * - Consulta os canais.
     * - Tenta criar um novo se houver menos de cinco.
     * - Adiciona uma inscrição se houver menos de três.
     */
    private List<String> prepararCanais(
            ZContext contexto,
            ZMQ.Socket inscrito,
            Set<String> assinaturas
    ) throws InterruptedException {

        List<String> canais = listarCanais(
                contexto,
                inscrito
        );

        if (canais == null) {
            return List.of();
        }

        if (canais.size() < 5) {

            int maiorNumero = 0;

            for (String canal : canais) {

                if (canal.matches("canal[0-9]+")) {

                    try {

                        int numero = Integer.parseInt(
                                canal.substring(5)
                        );

                        maiorNumero = Math.max(
                                maiorNumero,
                                numero
                        );

                    } catch (NumberFormatException ignored) {
                        // Ignora números que excedam o limite de int.
                    }
                }
            }

            String novoCanal = "canal" + (maiorNumero + 1);

            Map<String, Object> resposta = fazerPedido(
                    contexto,
                    inscrito,
                    "create_channel",
                    Protocolo.mapa(
                            "username", nomeBot,
                            "channel", novoCanal
                    )
            );

            if (resposta != null
                    && ("ok".equals(resposta.get("status"))
                    || "channel_exists".equals(
                            resposta.get("status")))) {

                if (!canais.contains(novoCanal)) {
                    canais.add(novoCanal);
                }

                canais.sort(String::compareTo);
            }
        }

        /*
         * Inscreve o bot em até três canais.
         */
        if (assinaturas.size() < 3) {

            List<String> opcoes = new ArrayList<>();

            for (String canal : canais) {

                if (!assinaturas.contains(canal)) {
                    opcoes.add(canal);
                }
            }

            if (!opcoes.isEmpty()) {

                String canal = opcoes.get(
                        random.nextInt(opcoes.size())
                );

                try {

                    /*
                     * O tópico deve ser serializado em MessagePack.
                     * Não podemos usar apenas os bytes UTF-8 do nome.
                     */
                    byte[] topico =
                            Protocolo.empacotar(canal);

                    inscrito.subscribe(topico);

                    assinaturas.add(canal);

                    System.out.println(
                            "[" + nomeBot
                                    + "] INSCRITO | canal: "
                                    + canal
                    );

                    /*
                     * Espera para permitir que a inscrição
                     * atravesse o proxy.
                     */
                    esperar(inscrito, 1);

                } catch (IOException e) {

                    System.err.println(
                            "Erro ao serializar o tópico: "
                                    + e.getMessage()
                    );
                }
            }
        }

        return canais;
    }

    /*
     * Fluxo principal do bot.
     */
    public void executar() throws InterruptedException {

        try (ZContext contexto = new ZContext()) {

            ZMQ.Socket inscrito =
                    contexto.createSocket(SocketType.SUB);

            inscrito.setLinger(0);
            inscrito.connect(enderecoInscricao);

            Set<String> assinaturas = new HashSet<>();

            try {

                /*
                 * Primeiro, realiza o login.
                 *
                 * Se houver falha, espera e tenta novamente.
                 */
                while (!Thread.currentThread().isInterrupted()) {

                    Map<String, Object> resposta = fazerPedido(
                            contexto,
                            inscrito,
                            "login",
                            Protocolo.mapa(
                                    "username", nomeBot
                            )
                    );

                    if (resposta != null
                            && "ok".equals(
                                    resposta.get("status"))) {
                        break;
                    }

                    esperar(inscrito, intervaloTentativas);
                }

                /*
                 * Três passagens iniciais para começar com
                 * até três inscrições.
                 */
                for (int i = 0; i < 3; i++) {

                    prepararCanais(
                            contexto,
                            inscrito,
                            assinaturas
                    );
                }

                /*
                 * Loop contínuo de publicação.
                 */
                while (!Thread.currentThread().isInterrupted()) {

                    List<String> canais = prepararCanais(
                            contexto,
                            inscrito,
                            assinaturas
                    );

                    if (canais.isEmpty()) {

                        esperar(
                                inscrito,
                                intervaloTentativas
                        );

                        continue;
                    }

                    /*
                     * Escolhemos um canal para o lote.
                     * Não precisa ser um canal inscrito.
                     */
                    String canal = canais.get(
                            random.nextInt(canais.size())
                    );

                    for (int numero = 0; numero < 10; numero++) {

                        long inicio = System.nanoTime();

                        fazerPedido(
                                contexto,
                                inscrito,
                                "publish",
                                Protocolo.mapa(
                                        "username", nomeBot,
                                        "channel", canal,
                                        "message", frases.get(
                                                random.nextInt(
                                                        frases.size()
                                                )
                                        )
                                )
                        );

                        double decorrido =
                                (System.nanoTime() - inicio)
                                        / 1_000_000_000.0;

                        /*
                         * Mantém aproximadamente um segundo
                         * entre o início das requisições,
                         * sem gerar rajadas para compensar atrasos.
                         */
                        esperar(
                                inscrito,
                                Math.max(
                                        0,
                                        intervaloMensagens - decorrido
                                )
                        );
                    }

                    receberPublicacoes(inscrito);
                }

            } finally {

                inscrito.close();
            }
        }
    }

    public static void main(String[] args) {

        Cliente cliente = new Cliente();

        System.out.println(
                "[" + cliente.nomeBot + "] Bot iniciado."
        );

        try {

            cliente.executar();

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            System.out.println(
                    "[" + cliente.nomeBot + "] Bot encerrado."
            );
        }
    }
}
