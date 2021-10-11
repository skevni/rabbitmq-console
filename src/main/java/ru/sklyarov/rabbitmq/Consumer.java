package ru.sklyarov.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private static final String EXCHANGE_NAME = "IT-blog";
    private static Map<String, String> mapOfConsumers;

    private static Connection connection;
    private static Channel channel;

    public static void main(String[] args) throws IOException, TimeoutException {
        mapOfConsumers = new HashMap<>();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // оборачивать в try-source не нужно, т.к. нам нужно ожидать callback
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String queueName = channel.queueDeclare().getQueue();

        Scanner scanner = new Scanner(System.in);
        System.out.println("At first, subscribe to the newsgroup\nExample, subscribe php");
        while (scanner.hasNext()) {
            String[] lineParts = scanner.nextLine().split("\\s+");
            if (lineParts.length == 2) {
                switch (lineParts[0]) {
                    case "subscribe":
                        queueChannelBind(queueName, lineParts[1]);
                        System.out.println("Subscribing to a newsgroup: " + lineParts[1]);
                        break;
                    case "unsubscribe":
                        queueChannelUnbind(queueName, lineParts[1]);
                        System.out.println("Unsubscribing from a newsgroup: " + lineParts[1]);
                        break;
                }
            } else {
                System.out.println("Invalid command. The command must have two arguments\nExample, subscribe php\nunsubscribe php");
            }
        }

    }

    private static void queueChannelBind(String queueName, String routingKey) throws IOException {
        if (channel != null) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);

            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                @Override
                public void handleCancel(String consumerTag) throws IOException {
                    super.handleCancel(consumerTag);
                }

                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                    super.handleShutdownSignal(consumerTag, sig);
                }

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String routingKey = envelope.getRoutingKey();
                    String contentType = properties.getContentType();
                    long deliveryTag = envelope.getDeliveryTag();
                    System.out.println(routingKey + " :: "+ new String(body, StandardCharsets.UTF_8));
                    //some operations
                    channel.basicAck(deliveryTag, false);
                }
            });
        }
    }

    private static void queueChannelUnbind(String queueName, String routingKey) throws IOException {
        if (channel != null) {

            channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);
        }
    }

}
