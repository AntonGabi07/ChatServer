import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;


public class Client implements AutoCloseable{



    private Connection connection;
    private Channel channel;
    private String username ;



    private static final String EXCHANGE_ROOM = "generalTopic";
    private static final String EXCHANGE_PRIVATE = "directTopic";
    private static final String EXCHANGE_SERVER = "serverTopic";
    private static final String SERVER_QUEUE = "serverQueue";



    public Client() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");



        connection = factory.newConnection();
        channel = connection.createChannel();



        channel.exchangeDeclare(EXCHANGE_ROOM, "topic");
        channel.exchangeDeclare(EXCHANGE_PRIVATE, "direct");



        channel.exchangeDeclare(EXCHANGE_SERVER, "topic");
    }




    public static void main(String[] args) throws Exception {
        try(Client client = new Client()){
            client.username = client.decideUsername();
            System.out.println("My name is "+ client.username);
        }catch(IOException | TimeoutException | InterruptedException e){
            e.printStackTrace();
        }

    }



    private String decideUsername() throws IOException, InterruptedException {
        //Set the username before loop
        Scanner s = new Scanner(System.in);
        System.out.print("type your name : ");
        String username = s.nextLine();



        //set random queueName and correlationID for RPC before loop




        //loop while username is already taken
        Boolean result = false;

        while(result == false){

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
            result = feedback.equals("true");
            channel.basicCancel(ctag);
            if(!result) {
                System.out.print("Username is already taken, type another : ");
                username = s.nextLine();
            }
        }
        return username;
    }



    public void close() throws IOException {
        connection.close();
    }



}