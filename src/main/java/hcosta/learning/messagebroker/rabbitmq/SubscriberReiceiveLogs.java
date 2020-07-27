package hcosta.learning.messagebroker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class SubscriberReiceiveLogs {

	private static final String EXCHANGE_NAME = "logs";

	private final static String HOST = "localhost";
	private final static int PORT = 5672;

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);
		
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		// Declare Exchange to no be null when started
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		// Declare a Queue for the Subscriber and associated to exchange
		String queueName = channel.queueDeclare().getQueue();
		channel.queueBind(queueName, EXCHANGE_NAME, "");

		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println(" [x] Received '" + message + "'");
		};
		channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
	}

}
