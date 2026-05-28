FROM maven:3.9.1-amazoncorretto-17 AS build

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

COPY ./pom.xml /usr/app/
COPY ./src /usr/app/src/

RUN mvn -f /usr/app/pom.xml verify

RUN mvn -f /usr/app/pom.xml -DskipTests clean package && cp /usr/app/target/messageproducer-*jar /docker-image.jar

FROM amazoncorretto:17.0.11-alpine
COPY --from=build /docker-image.jar /docker-image.jar

# Comando di avvio dell'applicazione
ENTRYPOINT ["sh", "-c", "java --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED -jar /docker-image.jar"]