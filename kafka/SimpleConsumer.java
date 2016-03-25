package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleConsumer {
	private final ConsumerConnector consumer;
	private final String topic;

	public SimpleConsumer(String zookeeper, String groupId, String topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(zookeeper,
						groupId));
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper,
			String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		//props.put("auto.offset.reset", "smallest");
		return new ConsumerConfig(props);
	}

	public void testConsumer() {
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		// Define single thread for topic
		topicMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer
				.createMessageStreams(topicMap);
		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap
				.get(topic);
		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			while (consumerIte.hasNext())
			{
				System.out.println("Message from Single Topic :: "
						+ new String(consumerIte.next().message()));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		}
		if (consumer != null)
			consumer.shutdown();
	}

	public static void main(String[] args) {
		String zooKeeper = args[0];
		String groupId = args[1];
		String topic = args[2];
		SimpleConsumer simpleHLConsumer = new SimpleConsumer(zooKeeper,
				groupId, topic);
		simpleHLConsumer.testConsumer();
	}
}