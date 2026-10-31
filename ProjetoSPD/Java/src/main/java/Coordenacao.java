import org.zeromq.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class Coordenacao implements AutoCloseable {
    private final ZContext contexto;
    private final RegistroServidor registro;
    private final AtomicInteger mensagens = new AtomicInteger(), pendentes = new AtomicInteger();
    private final AtomicBoolean eleicao = new AtomicBoolean();
    private final BlockingQueue<PedidoSync> fila = new LinkedBlockingQueue<>();
    private final Set<String> aplicadas = new HashSet<>();
    private volatile boolean parar;
    private volatile Map<String,Object> lider;
    private volatile Map<String,Map<String,Object>> servidores = Map.of();
    private volatile String ultimaRodada;
    private volatile long rodadas;
    private long aguardar;
    private Thread ouvinte, trabalhador;
    private final CountDownLatch pronto = new CountDownLatch(1);
    private volatile Exception erro;
    private record Rpc(Map<String,Object> resposta, long nanos) {}
    private record PedidoSync(String nome, CompletableFuture<Map<String,Object>> resultado) {}
    public Coordenacao(ZContext contexto, RegistroServidor registro) { this.contexto=contexto; this.registro=registro; }
    private static long numero(Object n) { return ((Number)n).longValue(); }
    private static boolean inteiro(Object n) { return n instanceof Integer || n instanceof Long || n instanceof Short || n instanceof Byte; }
    private static long agora() { return System.nanoTime()/1_000_000; }
    private boolean souLider() { var l=lider; return l!=null && registro.nome().equals(l.get("name")); }
    private Map<String,Object> eu() { return Protocolo.mapa("name",registro.nome(),"rank",registro.rank(),"address",registro.enderecoServidor()); }
    public void iniciar() throws Exception {
        ouvinte=new Thread(this::ouvir,"controle-java"); trabalhador=new Thread(this::trabalhar,"coordenacao-java");
        ouvinte.start();
        if(!pronto.await(3,TimeUnit.SECONDS) || erro!=null) { close(); throw new IllegalStateException("Falha ao abrir controle",erro); }
        trabalhador.start();
    }
    public void contarMensagem() { if(mensagens.incrementAndGet()>=15) { mensagens.addAndGet(-15); pendentes.incrementAndGet(); } }
    private synchronized boolean aceitar(Map<String,Object> c) {
        if(c==null || !(c.get("name") instanceof String) || !inteiro(c.get("rank")) || !(c.get("address") instanceof String a) || !a.startsWith("tcp://") || registro.rank()==0) return false;
        if(numero(c.get("rank"))<registro.rank()) { eleicao.set(true); return false; }
        if(lider!=null && numero(c.get("rank"))<numero(lider.get("rank"))) return false;
        boolean mudou=lider==null || !c.get("name").equals(lider.get("name")); lider=new LinkedHashMap<>(c);
        if(mudou) System.out.println("["+registro.nome()+"] COORDENADOR DEFINIDO | "+c);
        return true;
    }
    private Rpc rpc(String endereco,String op,Map<String,Object> dados,int timeout) {
        ZMQ.Socket s=contexto.createSocket(SocketType.REQ);
        try {
            s.setLinger(0); s.setSendTimeOut(timeout); s.setReceiveTimeOut(timeout); s.connect(endereco);
            var p=Protocolo.mapa("operation",op,"payload",dados); long inicio=System.nanoTime();
            synchronized(Protocolo.class) { Protocolo.prepararEnvio(p); if(!s.send(Protocolo.empacotar(p),0)) return null; }
            byte[] b=s.recv(0); long duracao=System.nanoTime()-inicio; if(b==null) return null;
            var r=Protocolo.desempacotarMapa(b); Protocolo.receberRelogio(r);
            if(!op.equals(r.get("operation")) || !Protocolo.timestampValido(r.get("timestamp")) || !(r.get("status") instanceof String) || Protocolo.comoMapa(r.get("payload"))==null) return null;
            return new Rpc(r,duracao);
        } catch(Exception e) { return null; } finally { s.close(); }
    }
    private static boolean ok(Rpc r) { return r!=null && "ok".equals(r.resposta.get("status")); }
    private Map<String,Object> status() { var l=lider; return Protocolo.mapa("name",registro.nome(),"rank",registro.rank(),"coordinator",l==null?null:l.get("name"),"time_ms",Protocolo.timestamp(),"adjustment_ms",Protocolo.ajusteAtual(),"rounds",rodadas,"last_round",ultimaRodada,"messages_since_sync",mensagens.get()); }
    private Map<String,Object> atender(Map<String,Object> p) throws Exception {
        String op=p.get("operation") instanceof String?(String)p.get("operation"):"";
        var d=Protocolo.comoMapa(p.get("payload")); String st="ok"; Map<String,Object> dados=Protocolo.mapa();
        if(d==null || !Protocolo.timestampValido(p.get("timestamp"))) return Protocolo.resposta(op,"invalid_request",dados);
        switch(op) {
            case "election":
                if(!inteiro(d.get("rank"))) st="invalid_request";
                else { if(registro.rank()>numero(d.get("rank"))) eleicao.set(true); dados=eu(); } break;
            case "coordinator": st=aceitar(d)?"ok":"lower_rank"; break;
            case "server_status": case "clock_read": dados=status(); break;
            case "coordinator_status": st=souLider()?"ok":"not_coordinator"; dados=status(); break;
            case "clock_adjust":
                synchronized(this) {
                    if(lider==null || !Objects.equals(d.get("coordinator"),lider.get("name")) || !inteiro(d.get("rank")) || numero(d.get("rank"))!=numero(lider.get("rank"))) st="not_coordinator";
                    else if(!(d.get("round_id") instanceof String) || !inteiro(d.get("adjustment_ms"))) st="invalid_request";
                    else { String id=(String)d.get("round_id"); if(aplicadas.add(id)) { Protocolo.aplicarAjuste(numero(d.get("adjustment_ms"))); ultimaRodada=id; System.out.println("["+registro.nome()+"] BERKELEY | correção: "+d.get("adjustment_ms")+"ms"); } dados=Protocolo.mapa("round_id",id); }
                } break;
            case "clock_sync":
                if(!souLider()) { st="not_coordinator"; dados=status(); }
                else if(!(d.get("name") instanceof String)) st="invalid_request";
                else { var futuro=new CompletableFuture<Map<String,Object>>(); fila.add(new PedidoSync((String)d.get("name"),futuro));
                    try { var r=futuro.get(15,TimeUnit.SECONDS); st=(String)r.get("status"); dados=Protocolo.comoMapa(r.get("payload")); } catch(TimeoutException e) { st="sync_timeout"; } }
                break;
            default: st="unknown_operation";
        }
        return Protocolo.resposta(op,st,dados);
    }
    private void ouvir() {
        ZMQ.Socket rep=contexto.createSocket(SocketType.REP),sub=contexto.createSocket(SocketType.SUB);
        try {
            rep.setLinger(0); sub.setLinger(0); rep.setReceiveTimeOut(30);
            if(!rep.bind(Protocolo.ambiente("ENDERECO_CONTROLE_BIND","tcp://*:5560"))) throw new IllegalStateException("Porta de controle indisponível");
            sub.connect(Protocolo.ambiente("ENDERECO_INSCRICAO","tcp://localhost:5558")); sub.subscribe(Protocolo.empacotar("servers")); pronto.countDown();
            while(!parar) {
                byte[] topico=sub.recv(ZMQ.DONTWAIT);
                if(topico!=null) {
                    byte[] corpo=sub.hasReceiveMore()?sub.recv(0):null;
                    boolean extra=sub.hasReceiveMore(); while(sub.hasReceiveMore()) sub.recv(0);
                    try { if(corpo!=null && !extra && Arrays.equals(topico,Protocolo.empacotar("servers"))) { var evento=Protocolo.desempacotarMapa(corpo); Protocolo.receberRelogio(evento); if("coordinator".equals(evento.get("operation"))) aceitar(Protocolo.comoMapa(evento.get("payload"))); } } catch(Exception ignored) {}
                }
                byte[] b=rep.recv(0); if(b==null) continue;
                Map<String,Object> resposta;
                try { var p=Protocolo.desempacotarMapa(b); Protocolo.receberRelogio(p); resposta=atender(p); }
                catch(Exception e) { resposta=Protocolo.resposta("","invalid_request",Protocolo.mapa()); }
                synchronized(Protocolo.class) { Protocolo.prepararEnvio(resposta); rep.send(Protocolo.empacotar(resposta),0); }
            }
        } catch(Exception e) { erro=e; parar=true; System.err.println("Erro no controle: "+e); }
        finally { pronto.countDown(); rep.close(); sub.close(); }
    }
    private void descobrir() {
        Rpc r=rpc(registro.enderecoReferencia(),"list_servers",Protocolo.mapa(),1000); if(!ok(r)) return;
        Object lista=Protocolo.comoMapa(r.resposta.get("payload")).get("servers"); if(!(lista instanceof List<?>)) return;
        Map<String,Map<String,Object>> novos=new LinkedHashMap<>();
        for(Object o:(List<?>)lista) { var s=Protocolo.comoMapa(o); if(s!=null && s.get("name") instanceof String n && inteiro(s.get("rank")) && s.get("address") instanceof String a && a.startsWith("tcp://")) novos.put(n,s); }
        var l=lider; if(l!=null) for(var e:novos.entrySet()) if(!servidores.containsKey(e.getKey()) && numero(e.getValue().get("rank"))>numero(l.get("rank"))) eleicao.set(true);
        servidores=novos;
    }
    private void anunciar(ZMQ.Socket pub,boolean confirmar) throws Exception {
        var evento=Protocolo.mapa("operation","coordinator","payload",eu());
        synchronized(Protocolo.class) { Protocolo.prepararEnvio(evento); pub.send(Protocolo.empacotar("servers"),ZMQ.SNDMORE); pub.send(Protocolo.empacotar(evento),0); }
        if(confirmar) for(var s:servidores.values()) if(!registro.nome().equals(s.get("name"))) rpc((String)s.get("address"),"coordinator",eu(),700);
    }
    private void eleger(ZMQ.Socket pub) throws Exception {
        eleicao.set(false); synchronized(this) { lider=null; }
        System.out.println("["+registro.nome()+"] ELEIÇÃO INICIADA | rank: "+registro.rank());
        boolean superior=false;
        for(var s:servidores.values()) if(numero(s.get("rank"))>registro.rank() && ok(rpc((String)s.get("address"),"election",eu(),700))) superior=true;
        if(superior) aguardar=agora()+3000; else if(aceitar(eu())) anunciar(pub,true);
    }
    // A média inclui o coordenador; rint segue o arredondamento para par do Python.
    static Map<String,Long> calcularCorrecoes(Map<String,Double> diferencas) {
        double media=diferencas.values().stream().mapToDouble(Double::doubleValue).average().orElseThrow();
        Map<String,Long> r=new LinkedHashMap<>(); diferencas.forEach((n,d)->r.put(n,(long)Math.rint(media-d))); return r;
    }
    private Map<String,Object> berkeley(String solicitante) {
        if(!souLider()) return Protocolo.resposta("clock_sync","not_coordinator",status());
        String rodada=UUID.randomUUID().toString(); Map<String,Double> dif=new LinkedHashMap<>(); dif.put(registro.nome(),0.0);
        Map<String,Map<String,Object>> participantes=new LinkedHashMap<>();
        for(var s:servidores.values()) { String n=(String)s.get("name"); if(n.equals(registro.nome())) continue;
            var r=rpc((String)s.get("address"),"clock_read",Protocolo.mapa(),700);
            if(ok(r)) { dif.put(n,numero(r.resposta.get("timestamp"))+r.nanos/2_000_000.0-Protocolo.timestamp()); participantes.put(n,s); }
        }
        if(!souLider()) return Protocolo.resposta("clock_sync","not_coordinator",status());
        var correcoes=calcularCorrecoes(dif); Set<String> confirmados=new HashSet<>(); confirmados.add(registro.nome());
        for(var e:participantes.entrySet()) if(ok(rpc((String)e.getValue().get("address"),"clock_adjust",Protocolo.mapa("coordinator",registro.nome(),"rank",registro.rank(),"round_id",rodada,"adjustment_ms",correcoes.get(e.getKey())),700))) confirmados.add(e.getKey());
        synchronized(this) { if(!souLider()) return Protocolo.resposta("clock_sync","not_coordinator",status()); Protocolo.aplicarAjuste(correcoes.get(registro.nome())); ultimaRodada=rodada; rodadas++; }
        System.out.println("["+registro.nome()+"] BERKELEY | correções: "+correcoes+" | confirmados: "+confirmados);
        boolean aplicado=confirmados.contains(solicitante);
        return Protocolo.resposta("clock_sync",aplicado?"ok":"sync_incomplete",Protocolo.mapa("coordinator",registro.nome(),"time_ms",Protocolo.timestamp(),"round_id",rodada,"applied",aplicado));
    }
    private synchronized void perder(Map<String,Object> anterior) { if(lider!=null && Objects.equals(lider.get("name"),anterior.get("name"))) { lider=null; eleicao.set(true); System.out.println("COORDENADOR INDISPONÍVEL | "+anterior.get("name")); } }
    private void trabalhar() {
        ZMQ.Socket pub=contexto.createSocket(SocketType.PUB); pub.setLinger(0);
        try {
            pub.connect(Protocolo.ambiente("ENDERECO_PUBLICACAO","tcp://localhost:5557")); long descobrirEm=0,verificarEm=0,anunciarEm=0;
            while(!parar) {
                if(registro.rank()==0) { Thread.sleep(50); continue; }
                long t=agora(); if(t>=descobrirEm) { descobrir(); descobrirEm=agora()+2000; }
                if(eleicao.get() || (lider==null && t>=aguardar)) eleger(pub);
                var l=lider;
                if(l!=null && souLider() && t>=anunciarEm) { anunciar(pub,false); anunciarEm=agora()+2000; }
                else if(l!=null && !souLider() && t>=verificarEm) { if(!ok(rpc((String)l.get("address"),"coordinator_status",Protocolo.mapa(),3000))) perder(l); verificarEm=agora()+2000; }
                var pedido=fila.poll(); if(pedido!=null) pedido.resultado.complete(berkeley(pedido.nome));
                if(lider!=null && pendentes.get()>0) { pendentes.decrementAndGet(); l=lider;
                    if(souLider()) berkeley(registro.nome());
                    else if(l!=null) { var r=rpc((String)l.get("address"),"clock_sync",Protocolo.mapa("name",registro.nome()),8000); if(r==null || "not_coordinator".equals(r.resposta.get("status"))) perder(l); }
                }
                Thread.sleep(30);
            }
        } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
        catch(Exception e) { erro=e; parar=true; System.err.println("Erro na coordenação: "+e); }
        finally { pub.close(); }
    }
    public void close() {
        parar=true;
        for(Thread t:new Thread[]{ouvinte,trabalhador}) if(t!=null && t.isAlive()) t.interrupt();
        for(Thread t:new Thread[]{ouvinte,trabalhador}) if(t!=null) try { t.join(); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
