import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.Timer;
import java.util.TimerTask;



public class Server implements AutoCloseable {

    class User {
        public String username;
        public long timestamp;

        User(String username, long timestamp){
            this.username=username;
            this.timestamp=timestamp;
        }
    }
    private Connection connection;
    private Channel channel;
    private static ArrayList<User> userList = new ArrayList<User>();
    Timer timer = new Timer();



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

            server.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Hello "+System.currentTimeMillis()+".");
                    server.verifyUsersStatus();
                }
            },0, 1000);

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

            DeliverCallback onlineCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    server.verifyUser(message);
                } finally {
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            server.channel.basicConsume(ONLINE_CHECK, false, onlineCallback, consumerTag -> { });
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

    protected void verifyUsersStatus() {
        for(User u : userList)
            if(System.currentTimeMillis()-u.timestamp > 1000){
                userList.remove(u);
                break;
            }
    }


    private String verifyUsername(String username){
        Boolean verify = userList.contains(username);
        if(verify){
            return "false";
        }
        userList.add(new User(username,System.currentTimeMillis()));
        return "true";
    }

    private static void verifyUser(String username) {
        long currentTime = System.currentTimeMillis();
        for(User u : userList){
            if(u.username.equals(username)){
                u.timestamp = currentTime;
                System.out.println("My name is "+u.username+" / "+u.timestamp);
                break;
            }
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
        timer.cancel();
    }
}