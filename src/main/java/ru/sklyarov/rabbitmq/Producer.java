package ru.sklyarov.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String EXCHANGE_NAME = "IT-blog";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            Scanner scanner = new Scanner(System.in);
            System.out.println("***\nWaiting for a message to be sent in format: routing message\nExample: php hello world!\nexit - This's the command to close the app\n***");

            while (scanner.hasNext()) {
                String text = scanner.nextLine();
                if (text.compareToIgnoreCase("выход") == 0 || text.compareToIgnoreCase("exit") == 0) {
                    break;
                }

                String[] textParts =  getRoutingKeyAndMessage(text);

                String routingKey = textParts[0];
                String message = textParts[1];


                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.printf("\"%s\" message has been sent to the \"%s\" routingKey\n", message, routingKey);
            }
        }
    }
    private static String[] getRoutingKeyAndMessage(String text) {
        int firstSpace = text.indexOf(' ');
        String[] parts = new String[2];

        parts[0] = "";
        parts[1] = text;

        if (firstSpace != -1) {
            parts[0] = text.substring(0, firstSpace);
            parts[1] = text.substring(firstSpace+1);
        }

        return parts;
    }
}
