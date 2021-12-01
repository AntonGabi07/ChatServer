import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.Timer;
import java.util.TimerTask;


class User {
    public String username;
    public long timestamp;

    User(String username, long timestamp){
        this.username=username;
        this.timestamp=timestamp;
    }
}

public class Server implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private static ArrayList<User> userList = new ArrayList<User>();
    Timer timer = new Timer();

    private static final String SERVER_QUEUE = "serverQueue";
    private static final String ONLINE_CHECK = "onlineQueue";
    private static final String PRIVATE_CHECK_ONLINE = "checkOnlineStatus";



    public Server() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(SERVER_QUEUE, false, false, false, null);
        channel.queuePurge(SERVER_QUEUE);
        channel.queueDeclare(ONLINE_CHECK, false, false, false, null);
        channel.queuePurge(ONLINE_CHECK);
        channel.queueDeclare(PRIVATE_CHECK_ONLINE, false, false, false, null);
        channel.queuePurge(PRIVATE_CHECK_ONLINE);
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


            Object monitor1 = new Object();
            DeliverCallback usernameCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                String response = "";
                try {
                    String username = new String(delivery.getBody(), "UTF-8");
                    System.out.println("[VERIFY] "+ username + " existance");
                    response = server.verifyUser(username);
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor1) {
                        monitor1.notify();
                    }
                }
            };

            Object monitor2 = new Object();
            DeliverCallback privateCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                String response = "";
                try {
                    String username = new String(delivery.getBody(), "UTF-8");
                    System.out.println("[PRIVATE] "+ username + " status");
                    response += server.verifyStatus(username);
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor2) {
                        monitor2.notify();
                    }
                }
            };

            DeliverCallback onlineCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    server.updateStatus(message);
                } finally {
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            

            server.channel.basicConsume(ONLINE_CHECK, false, onlineCallback, consumerTag -> { });
            server.channel.basicConsume(SERVER_QUEUE, false, usernameCallback, (consumerTag -> { }));
            server.channel.basicConsume(PRIVATE_CHECK_ONLINE, false, privateCallback, (consumerTag -> { }));

            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor1) {
                    try {
                        monitor1.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                synchronized (monitor2) {
                    try {
                        monitor2.wait();
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


    private String verifyUser(String username){
	Boolean verify=false;
	for(User u : userList){
            if(u.username.equals(username)){
                verify=true;
		break;
            }
	}
       
        if(verify){
            return "false";
        }
        userList.add(new User(username,System.currentTimeMillis()));
        return "true";
    }

    private String verifyStatus(String username){
        Boolean verify=false;
        for(User u: userList){
            if(u.username.equals(username)){
                verify=true;
                break;
            }
        }
        if(verify)
            return "true";
        return "false";
    }

    private static void updateStatus(String username) {
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