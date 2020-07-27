package hcosta.learning.messagebroker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class SubscriberDirectReceiverLogs {

	private final static String HOST = "localhost";
	private final static int PORT = 5672;

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		// Declare Exchange to no be null when started
		channel.exchangeDeclare(PublisherDirectEmitLog.EXCHANGE_NAME, "direct");

		if (argv.length < 1) {
			System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
			System.exit(1);
		}

		// Declare a Queue for the Subscriber and associated to exchange and add a routing key to filter
		String queueName = channel.queueDeclare().getQueue();

		for (String severity : argv) {
			channel.queueBind(queueName, PublisherDirectEmitLog.EXCHANGE_NAME, severity);
		}

		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
		};

		channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
	}

}
