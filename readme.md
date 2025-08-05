
mvn quarkus:dev -Dquarkus.profile=poc -DskipTests
mvn quarkus:dev -Dquarkus.profile=own


mvn quarkus:dev -Dquarkus.profile=poc -Dquarkus.http.port=8082
mvn quarkus:dev -Dquarkus.profile=poc -Dquarkus.http.port=8083