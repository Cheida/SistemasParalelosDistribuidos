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
│   ├── requirements.txt
│   └── Dockerfile
├── python/
│   ├── cliente.py
│   ├── servidor.py
│   ├── requirements.txt
│   └── Dockerfile
├── Java/
│   ├── cliente.java
│   ├── servidor.java
│   └── Dockerfile
└── C/
    ├── cliente.c
    ├── servidor.c
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

Até o momento, estão implementados o cliente e o servidor Python, o broker e a configuração inicial do Docker Compose. O bot faz login e alterna entre tentar criar canais novos com nomes sequenciais, como `canal1` e `canal2`, e tentar criar canais já listados para verificar o tratamento de duplicatas. Os pedidos e as respostas são exibidos com o timestamp e os dados da operação.

As implementações em Java e C, os testes de comunicação entre as três linguagens e as partes seguintes ainda estão em desenvolvimento. A presença das pastas no repositório não significa que todas as etapas já estejam concluídas.

Na versão atual, os servidores possuem bancos independentes e ainda não replicam os canais entre si. Por isso, a lista recebida por um bot depende do servidor que atender ao pedido.

Os bancos dos containers atuais ficam em volumes individuais anônimos. Parar e iniciar os mesmos containers preserva esses dados, mas remover os containers com `docker compose down` não permite que um novo `up` reassocie automaticamente os volumes antigos. A organização da persistência ainda precisa ser refinada para a entrega.

## Enunciado

O trabalho segue as orientações do [enunciado do projeto](https://gitlab.com/laferreira/fei/cc7261/-/blob/main/projeto/enunciado.md). Este README será atualizado conforme as próximas partes forem implementadas e integradas pelo grupo.
