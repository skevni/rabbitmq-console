package ru.sklyarov.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private static final String EXCHANGE_NAME = "IT-blog";

    private static Connection connection;
    private static Channel channel;

    public static void main(String[] args) throws IOException, TimeoutException {
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
        // TODO: создать set очередей, чтобы вывести в консоль все связанные очереди для их отвязывания.

    }

    private static void queueChannelBind(String queueName, String routingKey) throws IOException {
        if (channel != null) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("Received: '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        }
    }

    private static void queueChannelUnbind(String queueName, String routingKey) throws IOException {
        if (channel != null) {
            channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);
        }
    }

}
