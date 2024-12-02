FROM openjdk:17-jdk-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    maven \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Clone the rest_support branch
RUN git clone -b rest_support https://github.com/couchbaselabs/DocLoader.git /app/DocLoader

# Complile DocLoader
RUN cd DocLoader && mvn clean compile package

CMD ["java", "-jar", "/app/DocLoader/target/magmadocloader/magmadocloader.jar", "--server.port=49000"]