# Projeto de Sistemas Distribuídos

## Integrantes

- Gustavo Cheida
- Bruno Rossi
- Victor Ferraretto

## Sobre o trabalho

Este projeto está sendo desenvolvido para a disciplina de Sistemas Distribuídos. A proposta é construir um sistema de troca de mensagens em canais públicos, inspirado em serviços como BBS e IRC, colocando em prática os conceitos estudados nas aulas.

No nosso sistema, os usuários são representados por bots que fazem login, consultam os canais disponíveis e enviam pedidos aos servidores automaticamente. Ao longo do trabalho, vamos acrescentar a publicação de mensagens, a sincronização dos relógios, a eleição de um coordenador e a replicação dos dados entre os servidores.

O desenvolvimento está dividido entre Python, Java e C. Cada linguagem terá sua própria implementação de cliente e servidor, e todas deverão conseguir se comunicar usando o mesmo protocolo. A ideia é testar a comunicação entre programas diferentes, além de observar como o sistema se comporta com vários clientes e servidores funcionando ao mesmo tempo.

## Tecnologias e escolhas do grupo

| Tecnologia | Uso no projeto |
|---|---|
| Python, Java e C | Implementações dos clientes e servidores |
| ZeroMQ | Comunicação entre os processos |
| MessagePack | Serialização das mensagens em formato binário |
| SQLite | Armazenamento local dos dados de cada servidor |
| Docker e Docker Compose | Criação dos ambientes e execução dos serviços em containers |

Escolhemos MessagePack para usar um formato binário que possa ser lido nas três linguagens. Todas as mensagens devem conter o timestamp do envio, em milissegundos desde Unix epoch. O formato dos pedidos, das respostas e dos status está descrito em [docs/protocolo.md](docs/protocolo.md).

Para a persistência, usamos SQLite por permitir salvar os dados em disco sem precisar de um serviço de banco separado. Cada servidor deve manter seu próprio arquivo, sem compartilhar o mesmo banco com os demais. A sincronização dos dados entre os servidores será tratada na etapa de replicação.

## Etapas do projeto

O trabalho será construído de forma incremental, seguindo as cinco partes do enunciado:

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
│   ├── src/main/java/
│   │   ├── Cliente.java
│   │   ├── Servidor.java
│   │   ├── Protocolo.java
│   │   ├── RegistroServidor.java
│   │   └── Coordenacao.java
│   └── Dockerfile
└── C/
    ├── cliente.c
    ├── servidor.c
    ├── protocolo.c
    ├── protocolo.h
    ├── referencia.c
    ├── referencia.h
    ├── coordenacao.c
    ├── coordenacao.h
    └── Dockerfile
```

A pasta `infra` reúne o broker, que encaminha os pedidos dos clientes para os servidores e devolve as respostas. As pastas de cada linguagem ficam responsáveis pelas respectivas implementações. Os serviços das próximas etapas serão adicionados conforme o desenvolvimento.

## Como executar

É necessário ter Docker e Docker Compose disponíveis. No Mac e no Windows, usando Docker Desktop, ele deve estar funcionando em segundo plano.

Na pasta principal do projeto, execute:

```sh
docker compose up --build
```

Os serviços configurados no Compose serão iniciados e os bots começarão a realizar as operações sem precisar de interação pelo terminal. Para encerrar a execução, pressione `Control + C`.

A quantidade de clientes e servidores pode ser alterada no campo `scale` de cada serviço no `docker-compose.yaml`. As bibliotecas são instaladas durante a construção das imagens, então não é necessário instalar as dependências Python no computador para executar pelos containers.

## Andamento atual

As implementações Python, Java e C contêm login, canais, publicações, relógios lógicos, cadastro e heartbeat. As três linguagens também participam da parte 4: eleição do coordenador e sincronização pelo algoritmo de Berkeley. O serviço de referência, escrito em Python, fornece ranks e a lista de servidores ativos. A replicação da parte 5 já foi implementada em Python; Java e C serão integrados pelos respectivos responsáveis.

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

Cada servidor mantém seus próprios logins, canais, publicações e registros das trocas em SQLite. Os servidores Python já replicam essas quatro tabelas entre si. Até Java e C implementarem o protocolo da parte 5, as listas e históricos ainda podem variar entre linguagens. O bot escolhe canais que foram listados, e a publicação não depende de o canal existir no cadastro local do servidor que atendeu aquele pedido.

Todos os servidores usam volumes individuais anônimos. Parar e iniciar os mesmos containers mantém os dados; depois de remover os containers com `docker compose down`, um novo `up` não reassocia automaticamente esses volumes antigos. A referência usa o volume nomeado `dados_referencia`. As réplicas Java também têm nomes e bancos separados. O antigo volume compartilhado dados_servjava permanece declarado, mas não é montado nas novas réplicas e seus dados não são migrados automaticamente.

Pub/Sub não confirma recebimento individual pelos bots. A resposta `ok` à publicação indica que o servidor salvou e encaminhou o evento ao PUB, não que todos os assinantes o receberam. O histórico permanece armazenado, mas não há replay automático de publicações para bots que estavam desconectados.

## Enunciado

O trabalho segue as orientações do [enunciado do projeto](https://gitlab.com/laferreira/fei/cc7261/-/blob/main/projeto/enunciado.md). Este README será atualizado conforme as próximas partes forem implementadas e integradas pelo grupo.

## Comandos para apresentar a parte 4

Abra o terminal na pasta deste README (`/Users/gustavo.cheida/Documents/ProjetoSPD`). Inicie todos os serviços e reconstrua as imagens para carregar o código atual:

```sh
docker compose up --build -d
```

Em outro terminal, acompanhe **ao vivo** apenas os eventos de eleição de Python, Java e C:

```sh
docker compose logs -f --tail=0 servpy servjava servc | grep --line-buffered -E 'ELEIÇÃO INICIADA|COORDENADOR INDISPONÍVEL|COORDENADOR DEFINIDO|nome:|PUB servers'
```

`--tail=0` começa pelos eventos novos. Para ver o histórico de eleições já ocorridas:

```sh
docker compose logs --no-color servpy servjava servc | grep -E 'ELEIÇÃO INICIADA|COORDENADOR DEFINIDO|nome:|PUB servers' | tail -30
```

Para ver o último registro de coordenador definido (pode estar desatualizado se o processo parou):

```sh
docker compose logs --no-color servpy servjava servc | grep 'COORDENADOR DEFINIDO' | tail -1
```

A eleição só ocorre quando não há coordenador ou quando o coordenador atual fica indisponível. Veja os containers com `docker compose ps` e pare o **container específico** que foi eleito:

```sh
docker compose ps
docker stop NOME_OU_ID_DO_CONTAINER
```

Depois de alguns segundos, o terminal de acompanhamento deve mostrar a falha, a nova eleição e o novo coordenador. Para demonstrar a troca de linguagem, pare primeiro os servidores Java e depois os Python; o coordenador passa de Java para Python e, em seguida, para C, conforme os ranks dos servidores ativos:

```sh
docker compose stop servjava
docker compose stop servpy
```

Restaure os servidores após o teste:

```sh
docker compose up -d servjava servpy
```

`Control + C` encerra apenas o acompanhamento dos logs. Para encerrar todos os serviços, use `docker compose down`.

### Por que rank e clock crescem?

O **rank** é a ordem de cadastro persistida no banco da referência. Cada novo nome de servidor recebe um número maior, e os ranks antigos não são reutilizados. Recriar containers cria novos nomes, então o rank continua subindo.

O **clock** é um contador lógico de mensagens, não a hora do dia. Cada processo incrementa o contador ao enviar e atualiza seu valor ao receber um contador maior. Bots, respostas, publicações, heartbeat e coordenação fazem esse número crescer rapidamente. A sincronização de Berkeley ajusta o horário físico simulado (`timestamp`), não zera o clock lógico.

## Parte 5 — consistência e replicação em Python

A implementação Python usa **replicação assíncrona por log de eventos com anti-entropia**. Cada servidor continua gravando em seu próprio SQLite. Um processo de replicação no mesmo servidor captura novas linhas das tabelas `logins`, `canais`, `publicacoes` e `trocas`, registra eventos duráveis e consulta periodicamente os outros servidores. Cada réplica envia os eventos que o par ainda não viu. Ela também retransmite eventos recebidos, de modo que um servidor novo consegue recuperar dados mesmo se o produtor original já parou, desde que ao menos uma cópia sobreviva.

A replicação é **eventualmente consistente**: depois de uma gravação há um pequeno intervalo antes de todos os bancos terem a linha. Ela não confirma a gravação em todas as réplicas antes de responder ao cliente. Uma falha imediata do único servidor que recebeu uma mensagem ainda pode perdê-la antes da primeira cópia. A sincronização entre Python, Java e C ficará completa quando os responsáveis por Java e C implementarem o mesmo protocolo; o código das duas linguagens não foi alterado nesta etapa.

### Protocolo para Java e C

Cada servidor publica um endpoint ZeroMQ `REP` na porta **5561**, no mesmo hostname cadastrado na referência para a porta de controle 5560. Os pares descobrem os endereços por `list_servers` e consultam periodicamente o endpoint com MessagePack:

```text
REQ: {"desde": <último seq recebido deste par, ou 0>}
REP: {"eventos": [{"seq": inteiro, "origem": texto, "tipo": texto,
                   "chave": texto, "dados": binário MessagePack}, ...]}
```

A resposta traz até 100 eventos em ordem de `seq`. O consumidor grava cada evento e seu novo cursor na **mesma transação**. A identidade `(origem, tipo, chave)` impede duplicatas e ciclos de retransmissão. `tipo` é `logins`, `canais`, `publicacoes` ou `trocas`. `dados` é um mapa MessagePack com as colunas da tabela correspondente. Para `logins` e `trocas`, `chave` é o ID local do produtor, mas o receptor gera seu próprio ID SQLite. Para `publicacoes`, a chave é o UUID da publicação. Para `canais`, a chave é o nome; se dois servidores criarem o mesmo canal, vence a menor dupla `(timestamp, origem)`, de forma determinística. Nenhum servidor compartilha diretamente um arquivo de banco com outro.

A replicação tem seu próprio tráfego de controle; ele não conta para as 15 mensagens que disparam Berkeley. O servidor Python envia heartbeat periódico à referência para continuar visível mesmo sem pedidos de bots.

### Verificar as cópias Python

Depois de subir o projeto com `docker compose up --build -d`, use `docker compose ps servpy` para ver os nomes das duas réplicas. Compare, em cada uma, a quantidade de linhas das quatro tabelas (substitua `NOME_DO_CONTAINER`):

```sh
docker exec NOME_DO_CONTAINER python -c 'import sqlite3; b=sqlite3.connect("/app/dados/servpy.db"); print({t:b.execute("SELECT COUNT(*) FROM "+t).fetchone()[0] for t in ("logins","canais","publicacoes","trocas")})'
```

Com os bots ainda enviando mensagens, as contagens podem diferir por alguns segundos. Em bancos novos, quando pararem as gravações e a sincronização terminar, as duas réplicas Python devem ter as mesmas linhas. Bancos usados durante testes anteriores podem conter linhas duplicadas em `logins` e `trocas`; nesses casos, compare os registros distintos ou comece um teste com volumes novos apenas se puder descartar os dados antigos. Os logs de `servpy` mostram `RÉPLICA | ... eventos recebidos` quando chegam lotes.
