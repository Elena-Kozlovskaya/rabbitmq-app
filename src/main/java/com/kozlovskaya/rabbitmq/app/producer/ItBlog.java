package com.kozlovskaya.rabbitmq.app.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;


public class ItBlog {
    private static final String EXCHANGE_NAME = "programming_exchange";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);


            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                String routingKey;
                String message;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" ", 2);
                    if (parts.length == 2) {
                        routingKey = parts[0];
                        message = parts[1];
                        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
                        System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
