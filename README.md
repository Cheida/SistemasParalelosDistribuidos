# Projeto de Sistemas Distribuídos

## Integrantes

- Gustavo Cheida
- Bruno Rossi
- Victor Ferraretto

## Sobre o projeto

Este trabalho foi desenvolvido para a disciplina de Sistemas Distribuídos. A proposta é simular um sistema de mensagens em canais públicos, com bots que fazem login, consultam canais e enviam mensagens automaticamente.

Implementamos clientes e servidores em Python, Java e C, usando o mesmo protocolo para permitir a comunicação entre as linguagens. O projeto inclui troca de mensagens, relógios lógicos, eleição de coordenador, sincronização de relógios e replicação dos dados.

## Tecnologias

| Tecnologia | Uso |
|---|---|
| Python, Java e C | Clientes e servidores |
| ZeroMQ | Comunicação entre os processos |
| MessagePack | Serialização das mensagens |
| SQLite | Banco local de cada servidor |
| Docker e Docker Compose | Construção e execução dos containers |

Escolhemos MessagePack porque ele permite usar o mesmo formato binário nas três linguagens. As mensagens incluem um timestamp em milissegundos desde Unix epoch e um contador lógico chamado `clock`.

Cada servidor mantém seu próprio SQLite. Os dados são copiados entre os servidores pelo serviço de replicação, sem compartilhar diretamente o arquivo do banco.

Os detalhes das mensagens estão em [docs/protocolo.md](docs/protocolo.md).

## Etapas do trabalho

1. **Request-Reply:** login dos bots, criação e consulta de canais.
2. **Publisher-Subscriber:** inscrição em canais, publicação e recebimento de mensagens.
3. **Relógios e heartbeat:** contadores lógicos, ajuste do relógio físico e acompanhamento dos servidores.
4. **Eleição:** escolha do coordenador e sincronização pelo algoritmo de Berkeley.
5. **Consistência e replicação:** cópia dos dados para manter o histórico nos diferentes servidores.

## Organização do repositório

```text
ProjetoSPD/
├── README.md
├── docker-compose.yaml
├── docs/
│   └── protocolo.md
├── infra/
│   ├── broker.py
│   ├── proxy.py
│   ├── requirements.txt
│   └── Dockerfile
├── python/
│   ├── cliente.py
│   ├── servidor.py
│   ├── referencia.py
│   ├── registro.py
│   ├── coordenacao.py
│   ├── relogio.py
│   ├── replicacao.py
│   ├── requirements.txt
│   └── Dockerfile
├── Java/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/
│       ├── Cliente.java
│       ├── Servidor.java
│       ├── Protocolo.java
│       ├── RegistroServidor.java
│       ├── Coordenacao.java
│       └── Replicacao.java
└── C/
    ├── cliente.c
    ├── servidor.c
    ├── protocolo.c
    ├── protocolo.h
    ├── referencia.c
    ├── referencia.h
    ├── coordenacao.c
    ├── coordenacao.h
    ├── replicacao.c
    ├── replicacao.h
    └── Dockerfile
```

A pasta `infra` contém o broker, que distribui os pedidos entre os servidores, e o proxy das publicações. As pastas de cada linguagem reúnem os clientes, servidores e módulos de coordenação e replicação.

## Como executar e acompanhar

É necessário ter Docker e Docker Compose instalados. No Mac e no Windows, deixe o Docker Desktop aberto.

Abra os terminais na pasta que contém o arquivo `docker-compose.yaml`.

### 1. Iniciar o projeto

```sh
docker compose up --build
```

Por padrão, são iniciados broker, proxy, referência e dois clientes e dois servidores de cada linguagem. Os bots começam a operar automaticamente.

A quantidade de instâncias pode ser alterada no campo `scale` de cada serviço no Compose. As dependências são instaladas durante a construção das imagens.

### 2. Acompanhar eleição e sincronização

Em outro terminal:

```sh
docker compose logs -f --tail=100 servpy servjava servc | grep --line-buffered -E 'ELEIÇÃO|COORDENADOR|BERKELEY'
```

### 3. Consultar o último coordenador informado

```sh
docker compose logs --timestamps --no-color servpy servjava servc | grep -E 'PUB servers.*coordenador:|BERKELEY.*coordenador:|SINCRONIZAÇÃO.*coordenador:' | sort -k3 | tail -1
```

Esse comando mostra o registro mais recente que cita o coordenador. Se ele acabou de cair e a eleição ainda não terminou, o resultado pode estar desatualizado. Nesse caso, aguarde a eleição e execute novamente.

### 4. Acompanhar a replicação

```sh
docker compose logs -f --tail=100 servpy servjava servc | grep --line-buffered -E 'RÉPLICA|erro'
```

Linhas como `RÉPLICA | 100 eventos recebidos de ...` indicam que o servidor recebeu um lote de eventos de outra réplica. Isso mostra atividade de replicação, mas não comprova sozinho que todos os bancos já estão iguais.

Os comandos com `-f` continuam acompanhando novas linhas. Para sair, pressione `Control + C`. No terminal que executa o `docker compose up`, esse atalho encerra os serviços.

## Relógios, rank e heartbeat

O serviço de referência, escrito em Python, atende na porta 5559. Ele cadastra os servidores, atribui ranks e informa quais estão ativos.

O **rank** representa a ordem de cadastro. Cada novo nome recebe um número maior, que fica salvo no banco da referência. Recriar containers pode gerar nomes novos, por isso os ranks continuam aumentando.

O **clock** é um contador lógico. Cada processo incrementa seu valor ao enviar mensagens e o atualiza ao receber um contador maior. Como o sistema troca mensagens constantemente, esse número pode crescer rápido.

Os servidores enviam heartbeat depois de dez pedidos recebidos. A replicação também envia heartbeats periódicos, mantendo o cadastro ativo mesmo sem pedidos dos bots. O prazo de inatividade é definido por `PRAZO_HEARTBEAT`, com valor inicial de 60 segundos.

Para simular um relógio adiantado ou atrasado, usamos a variável `DESVIO_INICIAL_MS`. As correções alteram o horário usado pelo programa, sem mudar a hora do computador. Berkeley ajusta esse horário físico simulado, sem zerar o contador lógico.

## Parte 4 — eleição e Berkeley

A eleição foi implementada nas três linguagens seguindo a ideia do algoritmo Bully: o servidor com o maior rank entre os que responderem assume como coordenador. Quando ele fica indisponível, os demais podem iniciar outra eleição.

O vencedor anuncia seu nome no tópico `servers`. Cada servidor mantém o coordenador conhecido e usa a porta 5560 para as mensagens de controle.

Na sincronização de Berkeley, o coordenador coleta os horários dos participantes, calcula a média das diferenças e envia uma correção individual para cada um, incluindo ele mesmo.

A sincronização é agendada a cada quinze mensagens de atendimento, considerando pedidos, respostas e publicações. As mensagens de controle e de replicação ficam fora dessa contagem.

## Parte 5 — consistência e replicação

Usamos **replicação assíncrona por log de eventos com anti-entropia**. Na prática, cada servidor registra as alterações no próprio banco e consulta os outros periodicamente para buscar o que ainda não recebeu.

São replicados os dados de `logins`, `canais`, `publicacoes` e `trocas`. Um evento recebido também pode ser repassado, permitindo que uma nova instância recupere o histórico de uma réplica disponível.

Esse método resolve o problema do histórico dividido pelo balanceamento de carga: os dados deixam de ficar apenas no servidor que atendeu o pedido e passam a ser copiados para os demais.

A consistência é **eventual**. Os bancos podem ficar diferentes por alguns instantes, mas convergem conforme os eventos são trocados. O cliente recebe a resposta antes de todas as cópias serem confirmadas; se o servidor cair antes da primeira replicação, o dado ainda pode ser perdido.

### Protocolo de replicação

Cada servidor atende requisições ZeroMQ `REP` na porta 5561. Os endereços são descobertos pela operação `list_servers` da referência, usando o mesmo hostname cadastrado para a porta de controle.

As mensagens usam MessagePack:

```text
REQ: {"desde": <último seq recebido deste par, ou 0>}

REP: {"eventos": [{"seq": inteiro, "origem": texto, "tipo": texto,
                   "chave": texto, "dados": binário MessagePack}, ...]}
```

Cada resposta traz até 100 eventos em ordem de `seq`. O receptor salva os eventos e atualiza o cursor na mesma transação.

Para adaptar o método ao projeto, usamos a identidade `(origem, tipo, chave)` para impedir duplicatas e ciclos de retransmissão. O campo `dados` contém as colunas da tabela em formato MessagePack.

As chaves seguem estas regras:

- **Logins e trocas:** usam o ID do produtor como identificação do evento, mas recebem um novo ID no banco de destino.
- **Publicações:** usam o UUID da publicação.
- **Canais:** usam o nome. Se dois servidores criarem o mesmo canal, vence a menor dupla `(timestamp, origem)`, para que todos resolvam o conflito da mesma forma.

## Testes realizados

Na parte 3, testamos seis servidores e seis bots, com duas instâncias de cada linguagem, em uma rede Docker separada e com bancos temporários. Conferimos ranks únicos, heartbeats, contadores lógicos, gravação das trocas e desvios simulados de relógio de 60 segundos. Também verificamos clientes sendo atendidos por servidores de outras linguagens.

Na parte 4, testamos a troca de coordenador: ao parar o coordenador Java, Python assumiu; depois, ao parar os servidores Python, C foi eleito e publicou no tópico `servers`. Na implementação Python, também verificamos a sincronização após quinze mensagens e a convergência para a média de Berkeley.

Na parte 5, compilamos a versão C e observamos nos logs trocas de eventos entre Python, Java e C.

## Armazenamento e limitações

Cada servidor usa um volume individual anônimo. Parar e iniciar os mesmos containers mantém os dados. Depois de remover os containers com `docker compose down`, um novo `up` não reassocia automaticamente os volumes antigos.

A referência usa o volume nomeado `dados_referencia`. O volume antigo `dados_servjava` permanece declarado, mas não é montado nas réplicas atuais e seus dados não são migrados automaticamente.

As publicações via Pub/Sub não têm confirmação individual de recebimento. A resposta `ok` indica que o servidor salvou e encaminhou a mensagem, mas não garante que todos os bots a receberam. O histórico fica armazenado, porém não há reenvio automático para bots que estavam desconectados.

## Enunciado

O projeto segue o [enunciado da disciplina](https://gitlab.com/laferreira/fei/cc7261/-/blob/main/projeto/enunciado.md).
