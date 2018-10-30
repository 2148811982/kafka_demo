package com.zhong;

import java.util.Map;

import org.apache.kafka.common.Cluster;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class MyLogPartitioner implements Partitioner {

    public MyLogPartitioner(VerifiableProperties props) {
    }
    
	public MyLogPartitioner() {
	}

   /**
     *
     * @param obj
     * @param numPartitions 分区数
     * @return 分区号
     */
	public int partition(Object obj, int numPartitions) {
		return Math.abs(obj.hashCode())%numPartitions;
	}
}
