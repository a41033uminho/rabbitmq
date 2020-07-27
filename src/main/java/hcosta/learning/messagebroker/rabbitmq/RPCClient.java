package hcosta.learning.messagebroker.rabbitmq;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    public static final String RPC_QUEUE_NAME = "rpc_queue";

    
	private final static String HOST = "localhost";
	private final static int PORT = 5672;

    public RPCClient() throws IOException, TimeoutException {
    	//  establish a connection and channel.
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
    	// This structure allow us to automatically close RPCClient instance using close() method - implements AutoCloseable
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                // method makes the actual RPC request.
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException {
    	// first generate a unique correlationId number and save it
        final String corrId = UUID.randomUUID().toString();

        //create a dedicated exclusive queue for the reply and subscribe to it channel.basicConsume
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        //  publish the request message, with two properties: replyTo and correlationId.
        channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));

        // Since our consumer delivery handling is happening in a separate thread, we're going to need something to suspend the main thread before the response arrives.
        // Usage of BlockingQueue is one possible solutions to do so. Here we are 
        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        	// Test if corrId is the onw send it
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
            	// puts the response to BlockingQueue.
            	// main thread is waiting for response to take it from BlockingQueue.
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        };
        
        //create a dedicated exclusive queue for the reply and subscribe to it
		String ctag = channel.basicConsume(replyQueueName, true, deliverCallback , consumerTag -> {});

        String result = response.take();
        channel.basicCancel(ctag);
        return result;
    }

    public void close() throws IOException {
        connection.close();
    }
}

