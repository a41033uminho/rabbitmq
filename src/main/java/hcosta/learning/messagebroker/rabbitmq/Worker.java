package hcosta.learning.messagebroker.rabbitmq;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Worker {

	private final static String HOST = "localhost";
	private final static int PORT = 5672;

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		// queue will survive a RabbitMQ node restart if it is true
		boolean durable = true;

		channel.queueDeclare(NewTask.TASK_QUEUE_NAME, durable, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		// Max number of messages send to a Worker without receive feedback from last call
		int prefetchCount = 1;
		channel.basicQos(prefetchCount);

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println(" [x] Received '" + message + "'");
			try {
				doWork(message);
			} finally {
				System.out.println(" [x] Done");
				// Send Broker taht you complete the message processing withou errors
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		};
		boolean autoAck = false; // Auto send message for acknolegment

		CancelCallback cancelCallback = (cancelCall) -> {};

		channel.basicConsume(NewTask.TASK_QUEUE_NAME, autoAck, deliverCallback, cancelCallback );

	}

	private static void doWork(String task) {
		for (char ch : task.toCharArray()) {
			if (ch == '.') {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException _ignored) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

}
