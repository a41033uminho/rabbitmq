package hcosta.learning.messagebroker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PublisherFanoutEmitLog {

	private static final String EXCHANGE_NAME = "logs";

	private final static String HOST = "localhost";
	private final static int PORT = 5672;


	public static void main(String[] argv) throws Exception {
		// Message to be send
		String message = argv.length < 1 ? "info: Hello World!" : String.join(" ", argv);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);
		try (Connection connection = factory.newConnection();
				Channel channel = connection.createChannel()) {

			// Declare Exchange fanout and call it logs
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

			// Exchange declare instead of queue - as fanout was choosen the message goes to all queues
			channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));

			System.out.println(" [x] Sent '" + message + "'");
		}

	}

}
