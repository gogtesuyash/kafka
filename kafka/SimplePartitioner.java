package kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
	public SimplePartitioner(VerifiableProperties props) {
	}

	/*
	 * The method takes the key, which in this case is the IP address, It
	 * finds the last octet and does a modulo operation on the number of
	 * partitions defined within Kafka for the topic.
	 * 
	 * @see kafka.producer.Partitioner#partition(java.lang.Object, int)
	 */
	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		int partitionKey = (Integer.parseInt((String)key));
		partition = partitionKey % a_numPartitions;
		return partition;
	}
}