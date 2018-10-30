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
         * 1、指定当前kafka producer生产的数据的目的地
         *  创建topic可以输入以下命令，在kafka集群的任一节点进行创建。
         *  bin/kafka-topics.sh --create --zookeeper zk01:2181 --replication-factor 1 --partitions 1 --topic test
         */
		String TOPIC = "topic-";
		/**
         * 2、读取配置文件
         */
		Properties props = new Properties();
		/*
         * key.serializer.class默认为serializer.class
         * 序列化
         */
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		  /*
	       * kafka broker对应的主机，格式为host1:port1,host2:port2
	       * 任意一个节点都可以提供元数据信息
	       */
		props.put("metadata.broker.list", "192.168.3.9:9092");
		  /*
	       * request.required.acks,设置发送数据是否需要服务端的反馈,有三个值0,1,-1
	       * 0，意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。
	       * 这个选项提供了最低的延迟，但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
	       * 1，意味着在leader replica已经接收到数据后，producer会得到一个ack。
	       * 这个选项提供了更好的持久性，因为在server确认请求成功处理后，client才会返回。
	       * 如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
	       * -1，意味着在所有的ISR都接收到数据后，producer才得到一个ack。
	       * 这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失
	       */
		props.put("request.required.acks", "1");
	      /*
	       * 数据发送到哪些分区中，是由生产者来决定的，生产者根据某种消息分发策略将消息发送到各个分区中
	       * 可选配置，如果不配置，则使用默认的HashPartition
	       * 默认值：kafka.producer.DefaultPartitioner
	       * 用来把消息分到各个partition中，默认行为是对key进行hash。
	       */
		props.put("partitioner.class", "com.zhong.MyLogPartitioner");
//		props.put("key.serializer", "kafka.serializer.StringEncoder");
//		props.put("value.serializer", "kafka.serializer.StringEncoder");
//		        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		/**
         * 3、通过配置文件，创建生产者
         */
//		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
		/**
         * 4、通过循环生产数据
         */
		int messageNo = 0;
		String messageStr = messageNo + "   kafka";
        /*while (true){
            *//**
             * 5、调用producer的send方法发送数据
             * 注意：这里需要指定分区的Key，用来配合自定义的MyLogPartitioner进行数据分发
             * messageNo就是分区的key
             *//*
			// 错误情况：添加前缀 "a" 会出错
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
