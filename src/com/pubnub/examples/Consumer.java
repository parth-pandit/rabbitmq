package com.pubnub.examples;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

/*
 * Note this class is nearly identical to Worker.java in tutorial 2 on the RabbitMQ site from
 * http://www.rabbitmq.com/tutorials/tutorial-two-java.html
 * You can download the source code directly from 
 * http://github.com/rabbitmq/rabbitmq-tutorials/blob/master/java/Worker.java
 * The only real difference is that we use 2 different RabbitMQ channels to avoid collisions.
 * We use one channel to produce messages to and another to consume messages from
 * So make sure you set the constant TASK_QUEUE_NAME to something other than 'task_queue', 
 * such as 'task_queue_inbound_durable'
*/

public class Consumer {

	//messages subscribed from PubNub by producerServer for Consumer to consumePubNub
	private static String TASK_QUEUE_NAME = "to-alerts";
  private static String RMQ_HOST = "54.152.57.57";
  private static int RMQ_PORT = 5672;
  private static String RMQ_USER = "user";
  private static String RMQ_PASWD = "Abcd@1234";
  
  private static void loadEnvironment() {
    if (System.getenv("TASK_QUEUE_NAME") != null) {
      TASK_QUEUE_NAME = System.getenv("TASK_QUEUE_NAME");
    }
    if (System.getenv("RMQ_HOST") != null) {
      RMQ_HOST = System.getenv("RMQ_HOST");
    }
    if (System.getenv("RMQ_PORT") != null) {
      RMQ_PORT = Integer.valueOf(System.getenv("RMQ_PORT"));
    }
    if (System.getenv("RMQ_USER") != null) {
      RMQ_USER = System.getenv("RMQ_USER");
    }
    if (System.getenv("RMQ_PASWD") != null) {
      RMQ_PASWD = System.getenv("RMQ_PASWD");
    }
  }
    public static void main(String[] argv) throws Exception {
        loadEnvironment();
        System.out.println("Environment variables loaded");
        System.out.println("TASK_QUEUE_NAME: " + TASK_QUEUE_NAME);
        System.out.println("RMQ_HOST: " + RMQ_HOST);
        System.out.println("RMQ_PORT: " + Integer.toString(RMQ_PORT));
        System.out.println("RMQ_USER: " + RMQ_USER);
        System.out.println("RMQ_PASWD: " + RMQ_PASWD);
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(RMQ_USER);
        factory.setPassword(RMQ_PASWD);
        factory.setVirtualHost("/");
        factory.setHost(RMQ_HOST);
        factory.setPort(RMQ_PORT);

        Connection connection = factory.newConnection();
        System.out.println("created connection...");
        Channel channel = connection.createChannel();
        System.out.println("created channel...");
        //configure message queues as durable
        boolean durable = true;

        channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
        System.out.println(" [*] Consumer : waiting for messages. To exit press CTRL+C");

        //dispatch messages fairly rather than round-robin by waiting for ack before sending next message
        //be careful because queue can fill up if all workers are busy
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);
        //ensure that an explicit ack is sent from worker before removing from the queue
        boolean autoAck = false;

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);

        while (true) {
          QueueingConsumer.Delivery delivery = consumer.nextDelivery();
          String message = new String(delivery.getBody());
          
          System.out.println(" [x] Consumer : received '" + message + "'");
          System.out.println("\n\n");
          doWork(message);

          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
      }

      private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
          if (ch == '.') Thread.sleep(1000);
        }
      }
    }
