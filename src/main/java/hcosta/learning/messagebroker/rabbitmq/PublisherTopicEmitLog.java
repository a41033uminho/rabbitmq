package hcosta.learning.messagebroker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PublisherTopicEmitLog {

	public static final String EXCHANGE_NAME = "topic_logs";

	private final static String HOST = "localhost";
	private final static int PORT = 5672;


	public static void main(String[] argv) throws Exception {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);
		try (Connection connection = factory.newConnection();
				Channel channel = connection.createChannel()) {

			// Declare Exchange Topic and call it topic
			channel.exchangeDeclare(EXCHANGE_NAME, "topic");

			String routingKey = getRouting(argv);
			String message = getMessage(argv);

			//  declare  queue - as routingKey was choosen the message goes to all queues
			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));

			System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
		}

	}

	private static String getRouting(String[] strings) {
		if (strings.length < 1)
			return "anonymous.info";
		return strings[0];
	}

	private static String getMessage(String[] strings) {
		if (strings.length < 2)
			return "Hello World!";
		return joinStrings(strings, " ", 1);
	}

	private static String joinStrings(String[] strings, String delimiter, int startIndex) {
		int length = strings.length;
		if (length == 0) return "";
		if (length < startIndex) return "";
		StringBuilder words = new StringBuilder(strings[startIndex]);
		for (int i = startIndex + 1; i < length; i++) {
			words.append(delimiter).append(strings[i]);
		}
		return words.toString();
	}

}
