FROM maven:3.9.9-eclipse-temurin-21

WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn -B clean package

CMD ["mvn", "exec:java", "-Dexec.mainClass=Servidor"]
