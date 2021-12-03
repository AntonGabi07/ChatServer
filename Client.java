

import com.rabbitmq.client.*;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.Timer;
import java.util.TimerTask;


public class Client implements AutoCloseable{



    private Connection connection;
    private Channel channel;
    private String username ;
    Timer timer = new Timer();

    private static final String EXCHANGE_PRIVATE = "directTopic";
    private static final String SERVER_QUEUE = "serverQueue"; //used to connect to server; verify name uniqueness
    private static final String ONLINE_CHECK = "onlineQueue"; //used to send pings once every 500 ms
    private static final String PRIVATE_CHECK_ONLINE = "checkOnlineStatus";
    private static final String SHOW_ONLINE= "showOnline";
    private static final String CREATE_TOPIC= "createTopic";
    private static final String SEND_TO_TOPIC= "sendMessageToTopic";



    public Client() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_PRIVATE, "direct");
    }




    public static void main(String[] args) throws Exception {
        try(Client client = new Client()){
            Scanner s = new Scanner(System.in);
            client.username = client.decideUsername();

            System.out.println("Hello, "+client.username+". Welcome to the chat room!");
            helpDisplay();

            client.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run(){
                    try{
                        client.sendOnlineStatus(client.username);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            },0, 500);

            //CallBack when a message is consumed;
	        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
		        String message = new String(delivery.getBody(), "UTF-8");
		        System.out.println(message);
	    	};

	    	//random queue name :
        	String queueName = client.channel.queueDeclare().getQueue(); 

        	//Binding queue to our exchanges:
	        client.channel.queueBind(queueName, EXCHANGE_PRIVATE, client.username);

            client.channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

            boolean connected=true;
            while(connected){
                String message = s.nextLine();
                if(message.startsWith("--")){
                    switch(message.charAt(2)){
                        case 'p':
                            String[] params = message.split(" ", 3);
                            if(params.length < 3)
                                System.out.println("Usage: --p <Name> <Message>");	                        
                            else 
                                client.sendPrivateMessage(params[2], params[1]);
                            break;
                        case 't':
                            if(message.split(" ",3).length < 2){
                                System.out.println("Usage: --t <Name> <Duration>");	                        
                            }else
                                client.sendCreateTopic(message);
                            break;
                        case 'o':
                            client.getOnlineList(client.username);
                            break;
                        case 'q':
                            connected = false;
                            System.out.println("You disconnected from the chat.");
                            break;
                        case 'b':
                            client.sendToTopic(message);
                            break;
                        default:
                            System.out.println("Unknown command: ");
                            System.out.println("\t: "+message.split(" ")[0]);
	                        helpDisplay();
	                        break;

                    }
                }else{
                    System.out.println("Unknown command: ");
                    System.out.println("\t: "+message.split(" ")[0]);
                    helpDisplay();
                }
            }

        }catch(IOException | TimeoutException | InterruptedException e){
            e.printStackTrace();
        }

    }

    private void sendToTopic(String message) throws IOException, InterruptedException {
        channel.basicPublish("", SEND_TO_TOPIC, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes("UTF-8"));
    }




    private void sendCreateTopic(String str) throws IOException, InterruptedException {
        channel.basicPublish("", CREATE_TOPIC, MessageProperties.PERSISTENT_TEXT_PLAIN,str.getBytes("UTF-8"));
    }

    private void getOnlineList(String user) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();
        channel.basicPublish("", SHOW_ONLINE, props, user.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        String onlineList = response.take();
        channel.basicCancel(ctag);

        System.out.println(onlineList);
    }




    private void sendOnlineStatus(String username) throws IOException, InterruptedException {
        channel.basicPublish("", ONLINE_CHECK, MessageProperties.PERSISTENT_TEXT_PLAIN,username.getBytes("UTF-8"));
    }

    private String decideUsername() throws IOException, InterruptedException {
        //Set the username before loop
        Scanner s = new Scanner(System.in);
        System.out.print("Choose a username: ");
        String username = s.nextLine();

        //loop while username is already taken
        Boolean result = false;

        while(result == false){
            //set random queueName and correlationID for RPC before loop
            final String corrId = UUID.randomUUID().toString();
            String replyQueueName = channel.queueDeclare().getQueue();
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(corrId)
                    .replyTo(replyQueueName)
                    .build();
            channel.basicPublish("", SERVER_QUEUE, props, username.getBytes("UTF-8"));

            final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

            String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                    response.offer(new String(delivery.getBody(), "UTF-8"));
                }
            }, consumerTag -> {
            });



            String feedback = response.take();
            channel.basicCancel(ctag);
            result = feedback.equals("true");
            if(!result) {
                System.out.print("Username is already taken, type another : ");
                username = s.nextLine();
            }
        }
        return username;
    }

    private void sendPrivateMessage(String message, String dest) throws IOException, InterruptedException{

        if(dest.equals(username)){
            System.out.println("You can't send yourself messages");
            return;
        }

		final String corrId = UUID.randomUUID().toString();
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();
        channel.basicPublish("", PRIVATE_CHECK_ONLINE, props, dest.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        String isOnline = response.take();
        channel.basicCancel(ctag);

        String finalMessage="";
        if(isOnline.equals("true")){
            finalMessage ="Private message from " + username + ": " + message;
            channel.basicPublish(EXCHANGE_PRIVATE, dest, null, finalMessage.getBytes());
        }else{
            System.out.println("User "+dest+" is offline. Try another time");
        }

    }

    private static void helpDisplay(){
        System.out.println("********************  HELP  ********************");
        System.out.println("***** Timer for topic duration is optional *****");
        System.out.println("*  --o                    : Show online users  *");
        System.out.println("*  --p <name> <message>   : Private message    *");
        System.out.println("*  --t <name> <duration>  : Create a topic     *");
        System.out.println("*  --b <topic> <message>  : Write in topic     *");
        System.out.println("*  --l                    : List of topics     *");
        System.out.println("*  --s <topic name>       : Get topic messages *");
        System.out.println("*  --q                    : Leave the chat     *");
        System.out.println("************************************************\n");
    }

    public void close() throws IOException {
        timer.cancel();
        connection.close();
    }

}