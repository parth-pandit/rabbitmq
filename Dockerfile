FROM openjdk:11
WORKDIR /
ADD rabbitmq.jar rabbitmq.jar
CMD java -jar rabbitmq.jar
