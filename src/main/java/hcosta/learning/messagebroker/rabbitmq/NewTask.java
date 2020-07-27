package hcosta.learning.messagebroker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {

	public final static String TASK_QUEUE_NAME = "task_queue";
	
	private final static String HOST = "localhost";
	private final static int PORT = 5672;


	public static void main(String[] argv) throws Exception {
		 // Message to be send
		String message = String.join(" ", argv);
		
		// queue will survive a RabbitMQ node restart if it is true
		boolean durable = true;
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);
		try (Connection connection = factory.newConnection();
		     Channel channel = connection.createChannel()) {
			
			 // Declare a Queue
			 channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
			 // Publis message --  mark our messages as persistent MessageProperties.PERSISTENT_TEXT_PLAIN property
			 channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			 //
			 System.out.println(" [x] Sent '" + message + "'");
		}
		
	}

}
