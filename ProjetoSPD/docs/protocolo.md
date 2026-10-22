# Protocolo de comunicação

Como cada integrante vai desenvolver em uma linguagem diferente (Python, Java e C), precisamos combinar um formato de mensagem que funcione nas três. A ideia deste documento é deixar esse padrão registrado para todo mundo seguir durante a implementação.

Por enquanto, esta é uma proposta para a parte 1. Ainda precisamos confirmar as escolhas com o grupo.

## Como as mensagens vão ser enviadas

Vamos usar ZeroMQ com o padrão Request-Reply: o cliente faz um pedido e o servidor devolve uma resposta. Entre eles fica o broker, que encaminha as mensagens.

Os sockets ficam assim:

- Cliente: REQ.
- Broker: ROUTER do lado dos clientes e DEALER do lado dos servidores.
- Servidor: REP.

Para transformar as mensagens em binário, a proposta é usar MessagePack. Os exemplos deste arquivo estão escritos de um jeito que dá para ler, mas o programa deve converter os dados para MessagePack antes de enviar. Não vamos enviar JSON ou texto simples pela rede.

Cada pedido e cada resposta REQ/REP vão em um único frame binário. Os eventos Pub/Sub da parte 2 usam dois frames, conforme descrito no final deste documento. O broker deve encaminhar todos os frames recebidos, mantendo também os frames de roteamento do ZeroMQ, sem precisar interpretar o conteúdo.

As chaves dos mapas serão strings e os textos vão usar UTF-8. Aqui, “mapa” significa uma estrutura de chave e valor, como um dicionário em Python.

## Pedido do cliente

Todo pedido vai ter estes campos:

| Campo | Tipo | Para que serve |
|---|---|---|
| operation | string | O que o cliente quer fazer: login, create_channel, list_channels ou publish |
| timestamp | inteiro de 64 bits | Horário em que a mensagem foi enviada |
| payload | mapa | Dados necessários para aquela operação |

O `timestamp` será o número de milissegundos desde Unix epoch (01/01/1970, em UTC). Ele deve ser calculado antes de cada envio, tanto no cliente quanto no servidor. Assim, todo mundo usa a mesma unidade de tempo.

Um pedido de login teria esta estrutura:

```text
operation: "login"
timestamp: 1792530000000
payload:
  username: "bot-python-1"
```


## Resposta do servidor

A resposta vai seguir este formato:

| Campo | Tipo | Para que serve |
|---|---|---|
| operation | string | Mesma operação recebida no pedido |
| timestamp | inteiro de 64 bits | Horário de envio da resposta |
| status | string | ok quando deu certo; em caso de falha, o código do motivo em minúsculas |
| payload | mapa | Resultado da operação. Se der erro, fica vazio |

## Operações da parte 1

Nesta primeira parte, vamos implementar login, criação de canal e listagem de canais.

| Operação | O que vai no payload do pedido | O que volta no payload se der certo |
|---|---|---|
| login | username: string | username: string |
| create_channel | username: string, channel: string | channel: string |
| list_channels | mapa vazio | channels: lista de strings |
| publish | username: string, channel: string, message: string | channel: string, id: string |

No login, o bot informa só o nome, sem senha. Vamos aceitar novos logins com o mesmo nome e registrar cada um com seu timestamp. Nesta proposta, o nome identifica o bot, mas ainda não teremos controle de sessão ou autenticação. Se o login der erro, o bot deve esperar um intervalo antes de tentar novamente.

Para criar um canal, o cliente envia seu nome de usuário e o nome do canal. Se o canal já existir no servidor que recebeu o pedido, o servidor devolve um erro.

Na parte 2, o bot faz login e consulta os canais. A cada preparação de um lote, se a lista tiver menos de cinco canais, tenta criar um canal novo com nome sequencial. Se estiver inscrito em menos de três canais, escolhe aleatoriamente mais um canal ainda não assinado. Depois escolhe um canal disponível e envia um lote de dez mensagens aleatórias, com intervalo de um segundo entre o início dos pedidos quando o servidor consegue responder nesse tempo. Repete o processo sem interação do usuário. Durante esperas e pedidos, continua recebendo as publicações pelo SUB.

Os servidores ainda têm listas locais diferentes. Dois bots podem escolher o mesmo nome e receber channel_exists. O limite de cinco orienta a criação pelo bot; não é uma garantia global, pois há concorrência e ainda não existe replicação.

Na listagem, o servidor devolve os nomes dos canais em ordem alfabética dentro de `channels`. Se não houver nenhum canal, devolve uma lista vazia.

## Regras e erros

Para facilitar a implementação nas três linguagens, a proposta é aceitar nomes de usuário e de canal com 1 a 32 caracteres, usando apenas letras sem acento (A–Z e a–z), números, hífen e sublinhado. Maiúsculas e minúsculas serão diferentes: `Geral` e `geral`, por exemplo, serão nomes distintos.

Vamos usar os mesmos valores de status em todas as implementações. Sucesso é `ok`; os demais valores indicam a falha:

| Código | Quando usar |
|---|---|
| channel_exists | O canal já existe no servidor |
| invalid_name | O nome não segue as regras combinadas |
| invalid_request | Faltou algum campo obrigatório ou algum campo veio com o tipo errado |
| unknown_operation | A operação pedida não existe |
| internal_error | Aconteceu uma falha inesperada no servidor |
| invalid_message | Não foi possível decodificar a mensagem recebida |

Se não der para recuperar a `operation` de uma mensagem inválida, esse campo volta como string vazia na resposta.

## Armazenamento e próximas partes

A proposta é usar SQLite, com um banco separado para cada servidor. Os servidores não devem compartilhar o mesmo arquivo de banco. Na parte 1, vamos salvar os logins com seus timestamps e os canais criados.

Um ponto para acompanhar nos testes: como o broker distribui os pedidos, duas requisições do mesmo cliente podem chegar a servidores diferentes. Até implementarmos a replicação, os dados locais desses servidores podem ser diferentes.

Nas próximas partes, vamos atualizar este documento para incluir publicações, relógios, heartbeat, eleição e replicação. Sempre que o formato mudar, precisamos combinar a alteração entre os três para manter a comunicação funcionando.

## Como isso aparece no terminal

O registro mostra o timestamp que foi transmitido na mensagem, em milissegundos. Ele não é o horário em que a linha apareceu na tela. As operações são traduzidas só para facilitar a leitura: `list_channels` aparece como “listar canais” e `create_channel` como “criar canal”. Os campos dentro de payload aparecem como usuário, canal ou canais. Um payload vazio aparece como `dados: {}`.

Exemplo de pedido e resposta de login (valores ilustrativos):

```text
[bot-python-1] ENVIANDO | login | timestamp: 1792605936138 | usuário: bot-python-1
[servpy] RESPONDENDO | login | timestamp: 1792605937340 | usuário: bot-python-1 | status: ok
```

O prefixo entre colchetes e a ação (ENVIANDO, RECEBENDO ou RESPONDENDO) são informações do registro. Na rede, enviamos os campos das tabelas em MessagePack. Para ver também a estrutura completa, podemos configurar `MENSAGENS_COMPLETAS=1` no ambiente do container.

Cada socket REQ do cliente envia um pedido e aguarda sua resposta antes de ser fechado. Nesta versão não usamos version nem request_id. Python, Java e C devem seguir este mesmo formato simplificado.

Os nomes sequenciais facilitam os testes e não têm mais o limite de 100. A regra de até 32 caracteres por nome continua valendo. O servidor aceita qualquer nome válido, e os canais antigos não são apagados nem renomeados automaticamente.

A resposta agora possui somente operation, timestamp, status e payload. Não existe campo error: o próprio status informa o motivo da falha, por exemplo channel_exists. Em uma resposta de falha, payload é um mapa vazio.

## Parte 2 — publicação e inscrição

O broker REQ/REP continua nas portas 5555 e 5556. O novo proxy é separado: recebe os servidores PUB em XSUB na porta 5557 e atende os bots SUB em XPUB na porta 5558. Cada bot usa uma única conexão SUB para várias inscrições, escolhidas localmente; não existe pedido de inscrição ao servidor.

### Pedido de publicação

Usamos o mesmo envelope binário da parte 1:

```text
operation: publish
timestamp: 1792605936138
payload:
  username: bot-python-1
  channel: canal1
  message: Olá, canal!
```

O texto deve ter de 1 a 500 caracteres e não pode conter apenas espaços. O servidor valida o nome de usuário e do canal pelas regras da parte 1. Um texto inválido resulta em status invalid_message. Não usamos senhas nem sessões.

Como o broker pode entregar a publicação a um servidor diferente daquele que criou o canal, o servidor não exige que o canal exista no seu cadastro local. O bot escolhe o canal entre os nomes retornados pelo serviço. Esta é uma escolha temporária até a replicação; não há validação global de existência do canal.

O servidor salva a publicação em SQLite, envia o evento no PUB e só depois responde ao pedido:

```text
operation: publish
timestamp: 1792605936140
status: ok
payload:
  channel: canal1
  id: identificador-único-do-evento
```

O id é um UUID em string que identifica a publicação persistida. Não é um request_id do envelope. Em falha, seguimos o padrão status com o motivo e payload vazio.

### Evento publicado

A publicação tem dois frames, ambos em MessagePack:

1. Nome do canal serializado como string MessagePack (o tópico).
2. Mapa MessagePack com o evento abaixo.

```text
operation: publication
timestamp: 1792605936139
payload:
  id: identificador-único-do-evento
  username: bot-python-1
  channel: canal1
  message: Olá, canal!
  request_timestamp: 1792605936138
```

O timestamp do evento é gerado pelo servidor antes de persistir e encaminhar a publicação. request_timestamp preserva o horário enviado pelo bot. Cada assinante registra separadamente o timestamp local de recebimento e imprime canal, usuário, texto, horário do envio e do recebimento.

Para se inscrever, Java, C e Python devem usar os mesmos bytes MessagePack do nome do canal no filtro SUBSCRIBE. Por exemplo, canal1 é a sequência hexadecimal a6 63 61 6e 61 6c 31. O prefixo de comprimento da string evita confundir canal1 com canal10. Não enviar um tópico em texto UTF-8 puro nessa versão do protocolo.

Pub/Sub não confirma entrega a cada assinante. status ok significa que o servidor persistiu e encaminhou o evento ao socket PUB, não que todos os bots receberam. Inscrições levam tempo para se propagar; o bot faz uma espera inicial após cada nova inscrição, mas isso não oferece garantia de entrega. Não há replay automático do histórico nesta etapa.

### Armazenamento

Cada servidor mantém seu próprio SQLite. Além de logins e canais, a tabela publicacoes guarda o identificador, usuário, canal, texto, timestamps do pedido e da publicação e o evento MessagePack completo. A tabela trocas registra os bytes do pedido, a resposta em MessagePack e o timestamp de recebimento pelo servidor para todas as operações. As publicações são gravadas antes do envio no PUB. Não há exclusão automática nem réplica entre os bancos nesta etapa.
