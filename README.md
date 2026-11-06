# Projeto de Sistemas Distribuídos

## Integrantes

- Gustavo Cheida
- Bruno Rossi
- Victor Ferraretto

## Sobre o trabalho

Este projeto está sendo desenvolvido para a disciplina de Sistemas Distribuídos. A proposta é construir um sistema de troca de mensagens em canais públicos, inspirado em serviços como BBS e IRC, colocando em prática os conceitos estudados nas aulas.

No nosso sistema, os usuários são representados por bots que fazem login, consultam os canais disponíveis e enviam pedidos aos servidores automaticamente. O sistema também inclui publicação de mensagens, sincronização dos relógios, eleição de um coordenador e replicação dos dados entre os servidores.

O desenvolvimento está dividido entre Python, Java e C. Cada linguagem tem sua implementação de cliente e servidor, e todas se comunicam usando o mesmo protocolo. A ideia é testar a comunicação entre programas diferentes, além de observar como o sistema se comporta com vários clientes e servidores funcionando ao mesmo tempo.

## Tecnologias e escolhas do grupo

| Tecnologia | Uso no projeto |
|---|---|
| Python, Java e C | Implementações dos clientes e servidores |
| ZeroMQ | Comunicação entre os processos |
| MessagePack | Serialização das mensagens em formato binário |
| SQLite | Armazenamento local dos dados de cada servidor |
| Docker e Docker Compose | Criação dos ambientes e execução dos serviços em containers |

Escolhemos MessagePack para usar um formato binário que possa ser lido nas três linguagens. Todas as mensagens devem conter o timestamp do envio, em milissegundos desde Unix epoch. O formato dos pedidos, das respostas e dos status está descrito em [docs/protocolo.md](docs/protocolo.md).

Para a persistência, usamos SQLite por permitir salvar os dados em disco sem precisar de um serviço de banco separado. Cada servidor mantém seu próprio arquivo, sem compartilhar o mesmo banco com os demais. A parte 5 sincroniza os dados entre esses arquivos por replicação.

## Etapas do projeto

O trabalho foi desenvolvido em cinco partes:

1. **Request-Reply:** login dos bots, criação e listagem de canais e persistência dos logins e canais.
2. **Publisher-Subscriber:** inscrição em canais, publicação e recebimento de mensagens e armazenamento das publicações.
3. **Relógios e heartbeat:** relógios lógicos, sincronização do relógio físico e acompanhamento da disponibilidade dos servidores.
4. **Eleição:** escolha de um servidor coordenador e sincronização dos relógios usando o algoritmo de Berkeley.
5. **Consistência e replicação:** cópia dos dados entre os servidores para manter o histórico disponível nas diferentes instâncias.

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

A pasta `infra` contém o broker e o proxy de publicações. `python`, `Java` e `C` têm os clientes, servidores e arquivos de eleição e replicação de cada linguagem. `docs` explica o protocolo.

## Como executar

É necessário ter Docker e Docker Compose disponíveis. No Mac e no Windows, usando Docker Desktop, ele deve estar funcionando em segundo plano.

Na pasta principal do projeto, execute:

```sh
docker compose up --build
```

Os serviços configurados no Compose serão iniciados e os bots começarão a realizar as operações sem precisar de interação pelo terminal. Para encerrar a execução, pressione `Control + C`.

A quantidade de clientes e servidores pode ser alterada no campo `scale` de cada serviço no `docker-compose.yaml`. As bibliotecas são instaladas durante a construção das imagens, então não é necessário instalar as dependências Python no computador para executar pelos containers.

## Andamento atual

As implementações Python, Java e C contêm login, canais, publicações, relógios lógicos, cadastro e heartbeat. As três linguagens também participam da parte 4: eleição do coordenador e sincronização pelo algoritmo de Berkeley. O serviço de referência, escrito em Python, fornece ranks e a lista de servidores ativos. A replicação da parte 5 foi integrada em Python, Java e C, usando um protocolo comum na porta 5561.

## Como executar as versões atuais

```sh
docker compose up --build
```

A execução padrão inicia broker, proxy, referência e dois clientes e dois servidores de cada linguagem: Python, Java e C. Não é necessário ativar perfis ou executar os serviços separadamente. A quantidade de cada serviço pode ser alterada no campo scale do Compose.

Todos os pedidos, respostas e eventos das três linguagens têm o campo `clock`. O contador começa em zero, aumenta antes de enviar e recebe o máximo entre o valor local e o recebido, seguindo a regra do enunciado. O broker e o proxy continuam apenas encaminhando as mensagens, sem alterar os contadores.

O serviço `referencia` recebe os servidores na porta 5559, atribui ranks sem duplicar nomes e informa quais estão ativos. Cada servidor envia heartbeat depois de dez pedidos recebidos. Na parte 4, o ajuste do relógio físico vem do coordenador eleito, e não do timestamp da referência. O ajuste fica no programa e não muda a hora do computador.

O prazo de inatividade é configurado por `PRAZO_HEARTBEAT`, inicialmente 60 segundos. Como o heartbeat depende da quantidade de pedidos, um servidor ocioso também pode sair da lista quando esse prazo acabar. O rank fica reservado para o nome e é preservado no banco da referência. O formato completo e as escolhas estão em [docs/protocolo.md](docs/protocolo.md).

Para simular um relógio adiantado ou atrasado, a variável `DESVIO_INICIAL_MS` do servidor aceita uma diferença em milissegundos. Os registros mostram rank, heartbeat, correções de Berkeley e contadores das mensagens.

## Parte 4 — eleição e Berkeley

Os servidores Python, Java e C escolhem o coordenador pelo maior rank que responder, usando a ideia do algoritmo Bully. Se o coordenador ficar indisponível, os demais podem iniciar nova eleição. O vencedor anuncia seu nome no tópico `servers` do proxy Pub/Sub. O broker e o proxy continuam iguais.

Para sincronizar, o coordenador coleta os horários dos participantes, calcula a média das diferenças e envia correções individuais, incluindo a sua própria, seguindo Berkeley. A cada quinze mensagens de atendimento (pedidos, respostas e publicações), o servidor agenda uma sincronização. O tráfego de controle não entra nessa conta para evitar rodadas recursivas. O contador lógico continua presente em todas as mensagens.

Cada servidor abre uma porta de controle 5560 dentro do container e cadastra o endereço completo na referência. A referência mantém cadastro, rank e heartbeat; a correção do relógio físico vem do coordenador. O timestamp obrigatório dos envelopes continua existindo.

O mesmo `docker compose up --build` inicia as três linguagens, sem perfis opcionais. O protocolo de coordenação está descrito em [docs/protocolo.md](docs/protocolo.md).

## Verificação da parte 3

Testamos seis servidores e seis bots, sendo dois de cada linguagem, em uma rede Docker separada com bancos temporários. Conferimos ranks únicos, heartbeat, ajuste de desvios físicos simulados de 60 segundos, contadores nas requisições, respostas e publicações e gravação das trocas em cada banco. Também verificamos clientes Python, Java e C sendo atendidos por servidores das três linguagens.

## Verificação da parte 4

Testamos a eleição com servidores Python, Java e C no Docker. Ao parar o coordenador Java, Python assumiu; ao parar também os servidores Python, C assumiu e publicou no tópico `servers`. Também foram testadas, na implementação Python, a sincronização após quinze mensagens e a convergência para a média de Berkeley.

## Armazenamento e limitações atuais

Cada servidor mantém seu próprio SQLite e replica logins, canais, publicações e trocas por meio do protocolo comum da parte 5. A replicação é assíncrona; por alguns instantes, as listas e históricos podem variar enquanto os eventos são copiados. O bot escolhe canais que foram listados, e a publicação não depende de o canal existir no cadastro local do servidor que atendeu aquele pedido.

Todos os servidores usam volumes individuais anônimos. Parar e iniciar os mesmos containers mantém os dados; depois de remover os containers com `docker compose down`, um novo `up` não reassocia automaticamente esses volumes antigos. A referência usa o volume nomeado `dados_referencia`. As réplicas Java também têm nomes e bancos separados. O antigo volume compartilhado dados_servjava permanece declarado, mas não é montado nas novas réplicas e seus dados não são migrados automaticamente.

Pub/Sub não confirma recebimento individual pelos bots. A resposta `ok` à publicação indica que o servidor salvou e encaminhou o evento ao PUB, não que todos os assinantes o receberam. O histórico permanece armazenado, mas não há replay automático de publicações para bots que estavam desconectados.

## Enunciado

O trabalho segue as orientações do [enunciado do projeto](https://gitlab.com/laferreira/fei/cc7261/-/blob/main/projeto/enunciado.md). Este README documenta as partes implementadas e os comandos para executá-las durante a apresentação.

## Comandos para rodar o projeto SPD

Para apresentar, abrimos os terminais na pasta `ProjetoSPD`. No primeiro terminal, subimos tudo:

```sh
docker compose up --build
```

No segundo, acompanhamos a eleição e o ajuste dos relógios pelo algoritmo de Berkeley:

```sh
docker compose logs -f --tail=100 servpy servjava servc | grep --line-buffered -E 'ELEIÇÃO|COORDENADOR|BERKELEY'
```

No terceiro, consultamos o registro mais recente que informa o coordenador. Podemos rodar esse comando de novo depois de parar um servidor:

```sh
docker compose logs --timestamps --no-color servpy servjava servc | grep -E 'PUB servers.*coordenador:|BERKELEY.*coordenador:|SINCRONIZAÇÃO.*coordenador:' | sort -k3 | tail -1
```

No quarto, acompanhamos as réplicas em Python, Java e C:

```sh
docker compose logs -f --tail=100 servpy servjava servc | grep --line-buffered -E 'RÉPLICA|erro'
```

Os comandos com `-f` continuam mostrando linhas novas. Para sair deles, usamos `Control + C`. O terceiro comando mostra o último coordenador citado nos logs; ele pode estar desatualizado se o servidor acabou de cair e a eleição ainda não terminou.

### Por que rank e clock crescem?

O **rank** é a ordem de cadastro persistida no banco da referência. Cada novo nome de servidor recebe um número maior, e os ranks antigos não são reutilizados. Recriar containers cria novos nomes, então o rank continua subindo.

O **clock** é um contador lógico de mensagens, não a hora do dia. Cada processo incrementa o contador ao enviar e atualiza seu valor ao receber um contador maior. Bots, respostas, publicações, heartbeat e coordenação fazem esse número crescer rapidamente. A sincronização de Berkeley ajusta o horário físico simulado (`timestamp`), não zera o clock lógico.

## Parte 5 — consistência e replicação em Python, Java e C

Para resolver a perda de histórico, usamos **replicação assíncrona por log de eventos com anti-entropia** nas três linguagens. Cada servidor continua com seu próprio banco SQLite. As novas linhas de `logins`, `canais`, `publicacoes` e `trocas` entram em um log de eventos. Depois, cada servidor busca os eventos que ainda não recebeu dos outros. Um evento recebido também pode ser repassado, então um servidor novo consegue recuperar o histórico de outra réplica que ainda esteja funcionando.

Esse método é **eventualmente consistente**: depois de uma gravação, os outros bancos podem levar alguns segundos para receber a linha. O servidor responde ao cliente antes de confirmar todas as cópias. Por isso, se ele cair antes da primeira replicação, aquele dado ainda pode ser perdido. Compilamos a versão C e observamos nos logs trocas de eventos entre C, Java e Python.

### Protocolo comum às três linguagens

Cada servidor publica um endpoint ZeroMQ `REP` na porta **5561**, no mesmo hostname cadastrado na referência para a porta de controle 5560. Os pares descobrem os endereços por `list_servers` e consultam periodicamente o endpoint com MessagePack:

```text
REQ: {"desde": <último seq recebido deste par, ou 0>}
REP: {"eventos": [{"seq": inteiro, "origem": texto, "tipo": texto,
                   "chave": texto, "dados": binário MessagePack}, ...]}
```

A resposta traz até 100 eventos em ordem de `seq`. O consumidor grava cada evento e seu novo cursor na **mesma transação**. A identidade `(origem, tipo, chave)` impede duplicatas e ciclos de retransmissão. `tipo` é `logins`, `canais`, `publicacoes` ou `trocas`. `dados` é um mapa MessagePack com as colunas da tabela correspondente. Para `logins` e `trocas`, `chave` é o ID local do produtor, mas o receptor gera seu próprio ID SQLite. Para `publicacoes`, a chave é o UUID da publicação. Para `canais`, a chave é o nome; se dois servidores criarem o mesmo canal, vence a menor dupla `(timestamp, origem)`, de forma determinística. Nenhum servidor compartilha diretamente um arquivo de banco com outro.

A replicação tem seu próprio tráfego de controle; ele não conta para as 15 mensagens que disparam Berkeley. Os processos de replicação enviam heartbeat periódico à referência para manter os servidores visíveis mesmo sem pedidos de bots.

### Verificar as réplicas

Depois de subir o projeto com `docker compose up --build -d`, use `docker compose ps` para ver os nomes dos containers. Para conferir os logs de replicação de Python, Java e C, use o quarto comando da seção acima.

Com os bots enviando mensagens, os bancos podem ficar alguns instantes defasados; as mensagens `RÉPLICA | ... eventos recebidos de ...` mostram o servidor de origem de cada lote.
