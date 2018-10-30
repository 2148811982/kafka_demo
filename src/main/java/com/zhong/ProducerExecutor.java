package com.zhong;

import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;


public class ProducerExecutor implements Runnable {

	private Producer<String, String> producer;
	private final Integer messageNo;
	private final String topic;
	
	public ProducerExecutor(Producer<String, String> producer, int messageNo, String topic) {
		this.producer = producer;
		this.messageNo = messageNo;
		this.topic = topic;
	}


	@SuppressWarnings("deprecation")
	public void run() {
		String messageStr = messageNo + "   kafka";
		producer.send(new KeyedMessage<String, String>(topic,messageNo+"","appid:" + UUID.randomUUID() + messageStr));
		System.out.println(topic+","+messageNo);
	}

}
