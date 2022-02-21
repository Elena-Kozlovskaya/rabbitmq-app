package com.kozlovskaya.rabbitmq.app.consumer;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;


public class Subscriber {
    private static final String EXCHANGE_NAME = "programming_exchange";
    private static final String SET_TOPIC_COMMAND = "set_topic";
    private static final String DELETE_TOPIC_COMMAND = "delete_topic";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();
        System.out.println("QUEUE NAME: " + queueName);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            String routingKey;

            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                    if (parts[0].equals(SET_TOPIC_COMMAND)) {
                        routingKey = parts[1];
                        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                        System.out.println(" [*] Waiting for messages with routing key (" + routingKey + "):");

                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
                        };
                        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                        });
                    }
                    if (parts[0].equals(DELETE_TOPIC_COMMAND)) {
                        routingKey = parts[1];
                        channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);
                        System.out.println(" [*] Unsubscribed from messages with routing key (" + routingKey + "):");
                    }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
