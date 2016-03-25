package kafka;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;

public class SimpleProducer {
	private static Producer<String, String> producer;

	public SimpleProducer() {
		Properties props = new Properties();
		// Set the broker list for requesting metadata to find the lead broker
		props.put("metadata.broker.list", "hostname:portno");
		// This specifies the serializer class for keys
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 1 means the producer receives an acknowledgment once the leadreplica
		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request assuccessful.
		props.put("request.required.acks", "1");
		props.put("partitioner.class", "kafka.SimplePartitioner");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	public static void main(String[] args) {
		int argsCount = args.length;
		if (argsCount == 0 || argsCount == 1)
			throw new IllegalArgumentException(
					"Please provide topic name and Message count as arguments");
		// Topic name and the message count to be published is passed from the
		// command line
		String topic = (String) args[0];
		String count = (String) args[1];
		int messageCount = Integer.parseInt(count);
		System.out.println("Topic Name - " + topic);
		System.out.println("Message Count - " + messageCount);
		SimpleProducer simpleProducer = new SimpleProducer();
		simpleProducer.publishMessage(topic, messageCount);
	}

	private void publishMessage(String topic, int messageCount) {
		for (int mCount = 0; mCount < messageCount; mCount++) {
			String runtime = new Date().toString();
			String msg = "Message Number," + mCount;
			System.out.println(msg);
			// Creates a KeyedMessage instance
			KeyedMessage<String,String> data = new KeyedMessage(topic, String.valueOf(mCount), msg);
			// Publish the message
			producer.send(data);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Close producer connection with broker.
		producer.close();
	}
}