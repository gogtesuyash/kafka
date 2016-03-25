package kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class kafkaconsumer {

	
	private static Properties createConsumerConfig(String zookeeper,
			String groupId) {
		Properties props = new Properties();
		 props.put("bootstrap.servers", "rh7-from-vc.bmc.com:9092");
	     props.put("group.id", "groupname");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");
		return props;
	}
	
	public void testConsumer() {
		
		Properties props = createConsumerConfig("zookeeper-hostname", "groupname");
		 KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("topicname"));
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
	         for (ConsumerRecord<String, String> record : records)
	         {
	             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	             System.out.println();
	         }
	     }
	}
	
	public static void main(String[] args)
	{
		new kafkaconsumer().testConsumer();
	}
}
