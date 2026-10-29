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
│   ├── proxy.py
│   ├── broker.py
│   ├── proxy.py
│   ├── requirements.txt
│   └── Dockerfile
├── python/
│   ├── cliente.py
│   ├── servidor.py
│   ├── referencia.py
│   ├── registro.py
│   ├── relogio.py
│   ├── requirements.txt
│   └── Dockerfile
├── Java/
│   ├── pom.xml
│   ├── src/main/java/
│   │   ├── Cliente.java
│   │   ├── Servidor.java
│   │   ├── Protocolo.java
│   │   └── RegistroServidor.java
│   └── Dockerfile
└── C/
    ├── cliente.c
    ├── servidor.c
    ├── protocolo.c
    ├── protocolo.h
    ├── referencia.c
    ├── referencia.h
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

As implementações Python, Java e C contêm as funcionalidades das partes 1, 2 e 3: login, canais, publicações, relógios lógicos, cadastro de servidores e ranks, heartbeat e ajuste do relógio físico. O serviço de referência é compartilhado pelas três linguagens e escrito em Python.

Cada linguagem tem seu cliente e servidor e usa o mesmo protocolo MessagePack. A próxima etapa será a eleição de coordenador; a replicação dos bancos ainda será feita na parte 5.

## Parte 3 — como testar

```sh
docker compose up --build
```

A execução padrão inicia broker, proxy, referência e dois clientes e dois servidores de cada linguagem: Python, Java e C. Não é necessário ativar perfis ou executar os serviços separadamente. A quantidade de cada serviço pode ser alterada no campo scale do Compose.

Todos os pedidos, respostas e eventos das três linguagens têm o campo `clock`. O contador começa em zero, aumenta antes de enviar e recebe o máximo entre o valor local e o recebido, seguindo a regra do enunciado. O broker e o proxy continuam apenas encaminhando as mensagens, sem alterar os contadores.

O novo serviço `referencia` recebe os servidores na porta 5559, atribui ranks sem duplicar nomes e informa quais estão ativos. Cada servidor envia heartbeat depois de dez pedidos recebidos e usa o timestamp da resposta para ajustar o próprio relógio. Esse ajuste fica no programa e não muda a hora do computador.

O prazo de inatividade é configurado por `PRAZO_HEARTBEAT`, inicialmente 60 segundos. Como o heartbeat depende da quantidade de pedidos, um servidor ocioso também pode sair da lista quando esse prazo acabar. O rank fica reservado para o nome e é preservado no banco da referência. O formato completo e as escolhas estão em [docs/protocolo.md](docs/protocolo.md).

Para simular um relógio adiantado ou atrasado, a variável `DESVIO_INICIAL_MS` do servidor aceita uma diferença em milissegundos. Os registros mostram o rank, o heartbeat, a correção física e os contadores das mensagens.

## Verificação da parte 3

Testamos seis servidores e seis bots, sendo dois de cada linguagem, em uma rede Docker separada com bancos temporários. Conferimos ranks únicos, heartbeat, ajuste de desvios físicos simulados de 60 segundos, contadores nas requisições, respostas e publicações e gravação das trocas em cada banco. Também verificamos clientes Python, Java e C sendo atendidos por servidores das três linguagens.

## Armazenamento e limitações atuais

Cada servidor mantém seus próprios logins, canais, publicações e registros das trocas em SQLite. Ainda não existe replicação, portanto as listas de canais podem variar entre os servidores. O bot escolhe canais que foram listados, e a publicação não depende de o canal existir no cadastro local do servidor que atendeu aquele pedido.

Todos os servidores usam volumes individuais anônimos. Parar e iniciar os mesmos containers mantém os dados; depois de remover os containers com `docker compose down`, um novo `up` não reassocia automaticamente esses volumes antigos. A referência usa o volume nomeado `dados_referencia`. As réplicas Java também têm nomes e bancos separados. O antigo volume compartilhado dados_servjava permanece declarado, mas não é montado nas novas réplicas e seus dados não são migrados automaticamente.

Pub/Sub não confirma recebimento individual pelos bots. A resposta `ok` à publicação indica que o servidor salvou e encaminhou o evento ao PUB, não que todos os assinantes o receberam. O histórico permanece armazenado, mas não há replay automático nesta etapa.

## Enunciado

O trabalho segue as orientações do [enunciado do projeto](https://gitlab.com/laferreira/fei/cc7261/-/blob/main/projeto/enunciado.md). Este README será atualizado conforme as próximas partes forem implementadas e integradas pelo grupo.
