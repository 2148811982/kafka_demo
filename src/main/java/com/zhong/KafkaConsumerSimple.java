package com.zhong;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaConsumerSimple implements Runnable {
	private String title;
    private KafkaStream<byte[], byte[]>stream;
    private KafkaConsumerSimple(String title, KafkaStream<byte[], byte[]> stream) {
    	this.title = title;
	    this.stream = stream;
	}
	public void run() {
	    System.out.println("��ʼ���� " + title);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		/**
		         * ��ͣ�ش�stream��ȡ�µ�������Ϣ���ڵȴ��µ���Ϣʱ
		         * ������� `ConsumerConnector#shutdown`��hasNext()����������ô`hasNext`�᷵��false
		         * */
		while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> data = it.next();
            String topic = data.topic();
            int partition = data.partition();
            long offset = data.offset();
            String msg = new String(data.message());
            System.out.println(String.format(
            		"Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
            		title, topic, partition, offset, msg));
		}
		//System.err.println(String.format("Consumer: [%s] exiting ...", title));
	}
		
	public static void main(String[] args) throws Exception{
		// main����
		Properties props = new Properties();
		props.put("group.id", "testGroup");
		props.put("zookeeper.connect", "192.168.3.9:2181");
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("auto.offset.reset", "largest");
		props.put("auto.commit.interval.ms", "1000");
		props.put("partition.assignment.strategy", "roundrobin");
		ConsumerConfig config = new ConsumerConfig(props);
		String topic = "topic-8";
		//ֻҪConsumerConnector���ڵĻ���consumer��һֱ�ȴ�����Ϣ�������Լ��˳�
		ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(config);
		//����topicCountMap
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 2);
		//        topicCountMap.put(topic,10);
		
		Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamsMap = consumerConn.createMessageStreams(topicCountMap);
		//ȡ�� `kafkaTest` ��Ӧ�� streams
		List<KafkaStream<byte[], byte[]>> streams = topicStreamsMap.get(topic);
		//����һ���̳߳�
		ExecutorService executor = Executors.newFixedThreadPool(2);
		//����20��consumer threads
		for (int i = 0; i < streams.size(); i++)
			executor.execute(new KafkaConsumerSimple("������" + (i + 1), streams.get(i)));
	}

}
