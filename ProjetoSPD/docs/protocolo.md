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

Cada mensagem da aplicação vai em um único frame binário. O broker deve encaminhar todos os frames recebidos, mantendo também os frames de roteamento do ZeroMQ, sem precisar interpretar o conteúdo.

As chaves dos mapas serão strings e os textos vão usar UTF-8. Aqui, “mapa” significa uma estrutura de chave e valor, como um dicionário em Python.

## Pedido do cliente

Todo pedido vai ter estes campos:

| Campo | Tipo | Para que serve |
|---|---|---|
| operation | string | O que o cliente quer fazer: login, create_channel ou list_channels |
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

No login, o bot informa só o nome, sem senha. Vamos aceitar novos logins com o mesmo nome e registrar cada um com seu timestamp. Nesta proposta, o nome identifica o bot, mas ainda não teremos controle de sessão ou autenticação. Se o login der erro, o bot deve esperar um intervalo antes de tentar novamente.

Para criar um canal, o cliente envia seu nome de usuário e o nome do canal. Se o canal já existir no servidor que recebeu o pedido, o servidor devolve um erro.

Depois do login, o bot alterna entre tentar criar um nome novo e tentar criar um canal já listado. Para nomes novos, usa canal1, canal2, canal3 e assim por diante: pega o maior número listado ou já escolhido por ele e soma 1. Para tentar uma duplicata, sorteia um nome da lista. Se a lista estiver vazia, tenta um nome novo. Se não receber resposta da criação, mantém o tipo de tentativa para a próxima rodada.

Dois bots podem escolher o mesmo próximo número. Uma tentativa de duplicata também pode chegar a um servidor onde o canal ainda não existe. Por isso, a alternância das tentativas não garante alternância das respostas ok e channel_exists.

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
