FROM debian:bookworm-slim AS compilacao

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc libc6-dev libzmq3-dev libmsgpack-dev libsqlite3-dev uuid-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY protocolo.h protocolo.c servidor.c cliente.c ./

RUN gcc -std=c11 -O2 -Wall -Wextra -Werror -o servidor servidor.c protocolo.c \
        -lzmq -lmsgpackc -lsqlite3 -luuid \
    && gcc -std=c11 -O2 -Wall -Wextra -Werror -o cliente cliente.c protocolo.c \
        -lzmq -lmsgpackc -lsqlite3 -luuid

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libzmq5 libmsgpackc2 libsqlite3-0 libuuid1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=compilacao /src/servidor /src/cliente ./


CMD ["./servidor"]
