package com.zhong;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerSimple {
	public static void main(String[] args) throws InterruptedException {
		/**
         * 1��ָ����ǰkafka producer���������ݵ�Ŀ�ĵ�
         *  ����topic�����������������kafka��Ⱥ����һ�ڵ���д�����
         *  bin/kafka-topics.sh --create --zookeeper zk01:2181 --replication-factor 1 --partitions 1 --topic test
         */
		String TOPIC = "topic-";
		/**
         * 2����ȡ�����ļ�
         */
		Properties props = new Properties();
		/*
         * key.serializer.classĬ��Ϊserializer.class
         * ���л�
         */
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		  /*
	       * kafka broker��Ӧ����������ʽΪhost1:port1,host2:port2
	       * ����һ���ڵ㶼�����ṩԪ������Ϣ
	       */
		props.put("metadata.broker.list", "192.168.3.9:9092");
		  /*
	       * request.required.acks,���÷��������Ƿ���Ҫ����˵ķ���,������ֵ0,1,-1
	       * 0����ζ��producer��Զ����ȴ�һ������broker��ack�������0.7�汾����Ϊ��
	       * ���ѡ���ṩ����͵��ӳ٣����ǳ־û��ı�֤�������ģ���server�ҵ���ʱ��ᶪʧһЩ���ݡ�
	       * 1����ζ����leader replica�Ѿ����յ����ݺ�producer��õ�һ��ack��
	       * ���ѡ���ṩ�˸��õĳ־��ԣ���Ϊ��serverȷ������ɹ������client�Ż᷵�ء�
	       * �����д��leader�ϣ���û���ü�����leader�͹��ˣ���ô��Ϣ�ſ��ܻᶪʧ��
	       * -1����ζ�������е�ISR�����յ����ݺ�producer�ŵõ�һ��ack��
	       * ���ѡ���ṩ����õĳ־��ԣ�ֻҪ����һ��replica����ô���ݾͲ��ᶪʧ
	       */
		props.put("request.required.acks", "1");
	      /*
	       * ���ݷ��͵���Щ�����У������������������ģ������߸���ĳ����Ϣ�ַ����Խ���Ϣ���͵�����������
	       * ��ѡ���ã���������ã���ʹ��Ĭ�ϵ�HashPartition
	       * Ĭ��ֵ��kafka.producer.DefaultPartitioner
	       * ��������Ϣ�ֵ�����partition�У�Ĭ����Ϊ�Ƕ�key����hash��
	       */
		props.put("partitioner.class", "com.zhong.MyLogPartitioner");
//		props.put("key.serializer", "kafka.serializer.StringEncoder");
//		props.put("value.serializer", "kafka.serializer.StringEncoder");
//		        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		/**
         * 3��ͨ�������ļ�������������
         */
//		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
		/**
         * 4��ͨ��ѭ����������
         */
		int messageNo = 0;
		String messageStr = messageNo + "   kafka";
        /*while (true){
            *//**
             * 5������producer��send������������
             * ע�⣺������Ҫָ��������Key����������Զ����MyLogPartitioner�������ݷַ�
             * messageNo���Ƿ�����key
             *//*
			// ������������ǰ׺ "a" �����
            // producer.send(new KeyedMessage<String, String>(TOPIC,"a"+messageNo+"","appid" + UUID.randomUUID() + messageStr));
//			producer.send(new KeyedMessage<String, String>(TOPIC,messageNo+"","appid-" + UUID.randomUUID() + messageStr));
			messageNo += 1;
			Thread.sleep(2000);
		}*/
        
		Executor executor = Executors.newFixedThreadPool(100);
		long start = System.currentTimeMillis();
        for(int i = 0;i < 10;i++) {
        	executor.execute(new ProducerExecutor(producer,messageNo,TOPIC+i));
//        	producer.send(new KeyedMessage<String, String>(TOPIC,messageNo+"","appid-" + UUID.randomUUID() + messageStr));
        	++messageNo;
        }
        long end = System.currentTimeMillis();
        
        System.out.println("Total cost:"+(end-start));
        
    }

}
