package com.imi.storm.spout;

import java.util.Properties;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import com.imi.storm.Keys;

public class SpoutBuilder {

	public Properties configs = null;
	
	public SpoutBuilder(Properties configs){
		this.configs = configs;
	}
	
	public KafkaSpout buildKafkaSpout(){
				
		String zkUrl = configs.getProperty(Keys.KAFKA_ZOOKEEPER_HOST) + ":" + Integer.parseInt(configs.getProperty(Keys.KAFKA_ZOOKEEPER_PORT));		
		String topic = configs.getProperty(Keys.KAFKA_TOPIC);
		String zkRoot = configs.getProperty(Keys.KAFKA_ZKROOT);
		String groupId = configs.getProperty(Keys.KAFKA_CONSUMERGROUP);
		
		BrokerHosts hosts = new ZkHosts(zkUrl);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		return kafkaSpout;
	}
	
}
