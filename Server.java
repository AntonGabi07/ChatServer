import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
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

class Topic {
    public String topicName;
    private ArrayList<String> messages = new ArrayList<String>();

    public Topic(String topicName){
        this.topicName = topicName;
    }

    public String toString(){
        String result = "Topic '"+topicName + "':\n";
        for(String s : messages){
            result+= (s+"\n");
        }
        return result;
    }

    public void add(String message){
        messages.add(message);
    }
}

public class Server implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private static CopyOnWriteArrayList<User> userList = new CopyOnWriteArrayList<User>();
    private static CopyOnWriteArrayList<Topic> topics = new CopyOnWriteArrayList<Topic>();
    Timer timer = new Timer();

    private static final String SHOW_ONLINE= "showOnline";
    private static final String CREATE_TOPIC= "createTopic";
    private static final String SERVER_QUEUE = "serverQueue";
    private static final String ONLINE_CHECK = "onlineQueue";
    private static final String SEND_TO_TOPIC= "sendMessageToTopic";
    private static final String PRIVATE_CHECK_ONLINE = "checkOnlineStatus";
    private static final String GET_TOPIC = "getTopicMessages";
    private static final String LIST_TOPICS = "listTopics";




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
        channel.queueDeclare(SHOW_ONLINE, false, false, false, null);
        channel.queuePurge(SHOW_ONLINE);
        channel.queueDeclare(CREATE_TOPIC, false, false, false, null);
        channel.queuePurge(CREATE_TOPIC);
        channel.queueDeclare(SEND_TO_TOPIC, false, false, false, null);
        channel.queuePurge(SEND_TO_TOPIC);
        channel.queueDeclare(GET_TOPIC, false, false, false, null);
        channel.queuePurge(GET_TOPIC);
        channel.queueDeclare(LIST_TOPICS, false, false, false, null);
        channel.queuePurge(LIST_TOPICS);
        channel.basicQos(1);
    }


    public static void main(String[] args) throws Exception {
        
        try(Server server = new Server()){
            System.out.println(" [x] Awaiting RPC requests");

            server.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    server.verifyUsersStatus();
                }
            },0, 1000);


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
                }
            };

            DeliverCallback privateCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                String response = "";
                try {
                    String username = new String(delivery.getBody(), "UTF-8");
                    response += server.verifyStatus(username);
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
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

            DeliverCallback showOnlineCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                String response = "";
                try {
                    String username = new String(delivery.getBody(), "UTF-8");
                    response += server.getOnlineList(username);
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            DeliverCallback createTopicCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    server.createTopic(message);
                } finally {
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            DeliverCallback writeToTopicCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                .Builder()
                .correlationId(delivery.getProperties().getCorrelationId())
                .build();
                String response = "";
                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    response += server.writeToTopic(message);
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            DeliverCallback getTopicCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                String response = "";
                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    response += server.getTopicMessages(message);
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            DeliverCallback listTopicsCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                String response = "";
                try {
                    response += server.listTopics();
                } catch (RuntimeException e) {
                    System.out.println(" [ERROR] " + e.toString());
                } finally {
                    server.channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    server.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            server.channel.basicConsume(GET_TOPIC, false, getTopicCallback, consumerTag -> { });
            server.channel.basicConsume(ONLINE_CHECK, false, onlineCallback, consumerTag -> { });
            server.channel.basicConsume(SERVER_QUEUE, false, usernameCallback, consumerTag -> { });
            server.channel.basicConsume(LIST_TOPICS, false, listTopicsCallback, consumerTag -> { });
            server.channel.basicConsume(SHOW_ONLINE, false, showOnlineCallback, consumerTag -> { });
            server.channel.basicConsume(CREATE_TOPIC, false, createTopicCallback, consumerTag -> { });
            server.channel.basicConsume(SEND_TO_TOPIC, false, writeToTopicCallback, consumerTag -> { });
            server.channel.basicConsume(PRIVATE_CHECK_ONLINE, false, privateCallback, consumerTag -> { });

            // Wait and be prepared to consume the message from RPC client.
            while (true) {
            }
        }
        
    }

    private String listTopics(){
        String result="These are the available topics: \n";
        for(Topic t : topics){
            result+=("\t-"+t.topicName+"\n");
        }
        return result;
    }

    private String getTopicMessages(String message){
        String topicName = message.split(" ",2)[1];
        for(Topic t: topics){
            if(t.topicName.equals(topicName)){
                return t.toString();
            }
        }
        return "There is no topic named "+topicName+".";
    }

    private String writeToTopic(String message){
        String[] params = message.split(" ",3);
        for(Topic t : topics){
            if(t.topicName.equals(params[1])){
                t.add(params[0] + " said: " +params[2]);
                return "true";
            }
        }
        return "false";
    }

    private void createTopic(String arguments){
        String[] args = arguments.split(" ",3);
        for(Topic t : topics){
            if(t.topicName.equals(args[1])){
                System.out.println("Topic " +args[1]+ " exists.");
                return;
            }
        }
        topics.add(new Topic(args[1]));
        System.out.println("Created a topic named " + args[1]+".");
        if(args.length == 3){
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Destroying topic: "+args[1]+".");
                    destoryTopic(args[1]);
                }
            },Integer.parseInt(args[2])*60*1000);
        }else if(args.length == 2){
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Destroying topic: "+args[1]+".");
                    destoryTopic(args[1]);
                }
            },5*60*1000);
        }
    }

    protected void destoryTopic(String tName) {
        for(Topic t : topics){
            if(t.topicName.equals(tName)){
                topics.remove(t);
                break;
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
                break;
            }
        }
    }

    private static String getOnlineList(String user){
        String usersOnline = "These are the online users:\n";
        for(User u : userList){
            usersOnline+=("\t"+(u.username.equals(user)?"You":u.username)+"\n");
        }
        return usersOnline;
    }

    @Override
    public void close() throws Exception {
        connection.close();
        timer.cancel();
    }
}