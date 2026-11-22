FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/ventas-streams.jar /app/ventas-streams.jar
COPY --from=build /build/target/lib /app/lib
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/ventas-streams.jar"]
