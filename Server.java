import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;



public class Server implements AutoCloseable {


    private Connection connection;
    private Channel channel;
    private static ArrayList<String> userList = new ArrayList<String>();



    private static final String SERVER_QUEUE = "serverQueue";
    private static final String ONLINE_CHECK = "onlineQueue";


    public Server() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(SERVER_QUEUE, false, false, false, null);
        channel.queuePurge(SERVER_QUEUE);
        channel.queueDeclare(ONLINE_CHECK, false, false, false, null);
        channel.queuePurge(ONLINE_CHECK);
        channel.basicQos(1);
    }


    public static void main(String[] args) throws Exception {
        try(Server server = new Server()){
            System.out.println(" [x] Awaiting RPC requests");



            Object monitor = new Object();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                String response = "";
                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println("[VERIFY] "+ message + " existance");
                    response = server.verifyUsername(message);
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            };

            DeliverCallback onlineCallback = = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    server.verifyUsers(message);
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            channel.basicConsume(ONLINE_CHECK, false, onlineCallback, consumerTag -> { });
            server.channel.basicConsume(SERVER_QUEUE, false, deliverCallback, (consumerTag -> { }));
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    private String verifyUsername(String username){
        Boolean verify = userList.contains(username);
        if(verify){
            return "false";
        }
        userList.add(username);
        return "true";
    }

    private static void verifyUsers(String username) {
        for(String u : userList){
            if(u.equals(username)){
                System.out.println("My name is "+u+" and I'm online." + System.currentTimeMillis());
            }
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}