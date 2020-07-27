package hcosta.learning.messagebroker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Send {

	private final static String QUEUE_NAME = "hello";
	
	private final static String HOST = "localhost";
	private final static int PORT = 5672;


	public static void main(String[] argv) throws Exception {
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);
		try (Connection connection = factory.newConnection();
		     Channel channel = connection.createChannel()) {
			 // Declare a Queue
			 channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			 // Message to be send
			 String message = "Hello World!";
			 // Publis message
			 channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
			 //
			 System.out.println(" [x] Sent '" + message + "'");
		}
		
	}

}
