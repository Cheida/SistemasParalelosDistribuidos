# Protocolo de comunicação

Como cada integrante vai desenvolver em uma linguagem diferente (Python, Java e C), precisamos combinar um formato de mensagem que funcione nas três. A ideia deste documento é deixar esse padrão registrado para todo mundo seguir durante a implementação.

Este documento reúne o protocolo das partes 1 a 3. A parte 3 está implementada em Python, Java e C, com uma única referência compartilhada em Python.

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
| clock | inteiro de 64 bits | Contador lógico do processo que envia |
| timestamp | inteiro de 64 bits | Horário em que a mensagem foi enviada |
| payload | mapa | Dados necessários para aquela operação |

O `timestamp` será o número de milissegundos desde Unix epoch (01/01/1970, em UTC). Ele deve ser calculado antes de cada envio, tanto no cliente quanto no servidor. Assim, todo mundo usa a mesma unidade de tempo.

Um pedido de login teria esta estrutura:

```text
operation: "login"
timestamp: 1792530000000
clock: 1
payload:
  username: "bot-python-1"
```


## Resposta do servidor

A resposta vai seguir este formato:

| Campo | Tipo | Para que serve |
|---|---|---|
| operation | string | Mesma operação recebida no pedido |
| clock | inteiro de 64 bits | Contador lógico do processo que envia |
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

Este documento já inclui publicações, relógios e heartbeat. Nas próximas partes, vamos acrescentar eleição e replicação. Sempre que o formato mudar, precisamos combinar a alteração entre os três para manter a comunicação funcionando.

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

A resposta agora possui operation, timestamp, clock, status e payload. Não existe campo error: o próprio status informa o motivo da falha, por exemplo channel_exists. Em uma resposta de falha, payload é um mapa vazio.

## Parte 2 — publicação e inscrição

O broker REQ/REP continua nas portas 5555 e 5556. O novo proxy é separado: recebe os servidores PUB em XSUB na porta 5557 e atende os bots SUB em XPUB na porta 5558. Cada bot usa uma única conexão SUB para várias inscrições, escolhidas localmente; não existe pedido de inscrição ao servidor.

### Pedido de publicação

Usamos o mesmo envelope binário da parte 1:

```text
operation: publish
timestamp: 1792605936138
clock: 1
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
clock: 3
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
clock: 2
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

## Parte 3 — relógios e heartbeat

Todos os mapas de mensagem passam a ter o campo obrigatório `clock`, um inteiro não negativo de 64 bits. Isso vale para pedidos dos bots, respostas dos servidores, eventos publicados e pedidos/respostas da referência. O tópico do Pub/Sub é só o frame de roteamento; o contador fica no evento, e não se incrementa uma vez por frame.

Cada processo começa com contador zero. Antes de enviar uma mensagem, incrementa seu contador em 1 e inclui o resultado em clock. Ao receber, atualiza o contador local para max(local, recebido), sem somar 1 nesse recebimento, conforme o enunciado. A próxima mensagem enviada avança mais uma unidade. O mesmo servidor usa um só contador para REQ/REP dos bots, publicações e comunicação com a referência.

Exemplo: o bot envia clock 1, o servidor recebe e passa para pelo menos 1. Se publicar, envia o evento com clock 2; depois envia o REP com clock 3. Uma consulta à referência também pode aumentar esses valores, então eles não precisam ser consecutivos na tela do bot. Os registros mostram o contador transmitido; no recebimento de publicações, mostram também o contador local atualizado.

### Serviço de referência

Uma única instância em Python recebe sockets REQ dos servidores em REP na porta 5559, fora do broker. Os bots não usam essa conexão. Pedidos seguem o envelope da aplicação, com operation, timestamp, clock e payload; respostas acrescentam status. Os servidores devem usar nomes únicos entre todas as réplicas e linguagens.

| operation | payload do pedido | payload da resposta de sucesso |
|---|---|---|
| register_server | name: string, address: string | name: string, rank: inteiro positivo |
| list_servers | mapa vazio | servers: lista de mapas com name, rank e address |
| heartbeat | name: string | name: string, rank: inteiro positivo |

Exemplo de cadastro:

```text
operation: register_server
timestamp: 1792605936138
clock: 1
payload:
  name: servpy-a1b2c3
  address: a1b2c3
```

O address é o hostname alcançável na rede dos containers; nesta etapa não representa uma porta de atendimento direto implementada pelo servidor. O nome do processo e seu endereço ficam associados ao rank. O mesmo nome recebe o mesmo rank e atualiza seu endereço. Os ranks ficam em SQLite próprio da referência, no volume nomeado dados_referencia. A referência mantém apenas em memória os últimos sinais de atividade; depois de reiniciá-la, a lista ativa é reconstruída com novos cadastros e heartbeats. Um nome conhecido pode voltar por heartbeat e manter seu rank.

O horário correto vem no timestamp da resposta ao heartbeat, que já é obrigatório no envelope. Um heartbeat de nome nunca cadastrado recebe status server_not_registered e payload vazio; o servidor deve fazer register_server novamente. Outros status possíveis são invalid_request, invalid_name, unknown_operation e internal_error.

### Contagem e disponibilidade

Cada servidor conta seus próprios pedidos de clientes recebidos pelo REP, inclusive inválidos. Após responder ao décimo, envia um heartbeat e zera a contagem. Pedidos feitos à referência e eventos PUB não entram nessa conta. Não adicionamos heartbeat periódico por tempo nesta implementação.

A referência remove da lista ativa nomes sem cadastro ou heartbeat nos últimos 60 segundos. Esse prazo é configurado por PRAZO_HEARTBEAT e verificado com relógio monotônico, mesmo quando não chegam pedidos. Não se apaga o rank reservado daquele nome.

Como o enunciado usa heartbeat por quantidade de mensagens, um servidor vivo que fique ocioso por mais tempo que o prazo também sairá da lista. Isso é uma limitação do critério de ausência de heartbeat, não uma prova de que o processo morreu. Ajuste o prazo à carga dos testes. Ao voltar a receber dez pedidos, ele pode retornar à lista pelo heartbeat. Não há ainda eleição, heartbeat por timer ou tolerância à falha da própria referência.

### Ajuste do relógio físico

O servidor usa timestamp = hora do sistema + ajuste_ms. Quando recebe a resposta de heartbeat, calcula um ajuste estimando metade do tempo de ida e volta como atraso da resposta. Assim corrige apenas o relógio usado pelo processo, sem alterar o relógio do Mac ou o relógio do host Docker. É uma aproximação que supõe atrasos semelhantes nos dois sentidos, não uma sincronização exata.

Para demonstrar a correção, pode-se iniciar um servidor com DESVIO_INICIAL_MS, por exemplo 60000. Depois do heartbeat, o ajuste passa a ser calculado a partir da referência. Uma correção pode fazer o timestamp físico recuar; o clock lógico continua sem diminuir.

### Integração das linguagens

O serviço de referência é compartilhado e foi escrito em Python. Clientes e servidores Python, Java e C usam clock no envelope. Os servidores das três linguagens cadastram nome e endereço, guardam rank, consultam a lista e enviam heartbeat com ajuste físico. Todos usam a mesma regra de incrementar ao enviar e aplicar max ao receber.

O Compose inicia as três linguagens sem perfis opcionais. Cada réplica tem nome e banco próprios, incluindo Java. Mensagens sem clock são inválidas no protocolo da parte 3. O broker e o proxy não foram alterados.
