package com.imi.storm;

public class Keys {

	public static final String TOPOLOGY_NAME = "topology";
	
	//kafka spout
	public static final String KAFKA_SPOUT_ID = "kafka-spout";
	public static final String KAFKA_ZOOKEEPER_HOST = "kafka.zookeeper.host";
	public static final String KAFKA_ZOOKEEPER_PORT = "kafka.zookeeper.port";
	public static final String KAFKA_TOPIC = "kafka.topic";
	public static final String KAFKA_ZKROOT = "kafka.zkRoot";
	public static final String KAFKA_CONSUMERGROUP = "kafka.consumer.group";
	public static final String KAFKA_SPOUT_CONT = "kafkaspout.count";
	
	//storm setting
	public static final String STORM_NIMBUS_HOST = "storm.nimbus.host";
	public static final String STORM_NIMBUS_PORT = "storm.nimbus.port";
	
	//mongo db
	public static final String MONGO_BOLT_ID = "mongodb.bolt.id";
	public static final String MONGO_HOST = "mongodb.host";
	public static final String MONGO_PORT = "mongodb.port";
	public static final String MONGO_DATABASE = "mongodb.database";
	public static final String MONGO_COLLECTION = "mongodb.collection";
	public static final String MONGO_BOLT_COUNT = "mongodb.bolt.count";
	
	
}
