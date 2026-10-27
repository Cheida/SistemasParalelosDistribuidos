import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Protocolo {

    private Protocolo() {
    }

  
    public static final ObjectMapper MAPPER =
            new ObjectMapper(new MessagePackFactory());

  
    public static long timestamp() {
        return System.currentTimeMillis();
    }

  
    public static byte[] empacotar(Object objeto) throws IOException {
        return MAPPER.writeValueAsBytes(objeto);
    }

  
    public static Map<String, Object> desempacotarMapa(byte[] bytes)
            throws IOException {

        return MAPPER.readValue(
                bytes,
                new TypeReference<Map<String, Object>>() {
                }
        );
    }

   
    public static Map<String, Object> mapa(Object... pares) {

        if (pares.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "A quantidade de argumentos deve ser par."
            );
        }

        Map<String, Object> resultado = new LinkedHashMap<>();

        for (int i = 0; i < pares.length; i += 2) {

            String chave = (String) pares[i];
            Object valor = pares[i + 1];

            resultado.put(chave, valor);
        }

        return resultado;
    }

   
    @SuppressWarnings("unchecked")
    public static Map<String, Object> comoMapa(Object objeto) {

        if (objeto instanceof Map<?, ?>) {
            return (Map<String, Object>) objeto;
        }

        return null;
    }

    
    public static String ambiente(
            String nome,
            String valorPadrao
    ) {

        String valor = System.getenv(nome);

        if (valor == null || valor.isBlank()) {
            return valorPadrao;
        }

        return valor;
    }

  
    public static boolean nomeValido(String nome) {

        if (nome == null) {
            return false;
        }

        return nome.matches("[A-Za-z0-9_-]{1,32}");
    }

    
    public static boolean timestampValido(Object valor) {

        if (!(valor instanceof Number numero)) {
            return false;
        }

        if (valor instanceof Float || valor instanceof Double) {
            return false;
        }

        long timestamp = numero.longValue();

        return timestamp >= 0;
    }

   
    public static Map<String, Object> resposta(
            String operacao,
            String status,
            Map<String, Object> payload
    ) {

        return mapa(
                "operation", operacao,
                "timestamp", timestamp(),
                "status", status,
                "payload", payload == null
                        ? mapa()
                        : payload
        );
    }
}
