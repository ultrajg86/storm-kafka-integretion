package com.imi.simple;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

import com.google.common.base.Joiner;
import com.imi.simple.bolt.BoltBuilder;
import com.imi.simple.bolt.GroupBolt;
import com.imi.simple.bolt.MongodbBolt;
import com.imi.simple.bolt.MysqlBolt;
import com.imi.simple.spout.SpoutBuilder;

public class Topology {
	
	public Properties configs;
	public BoltBuilder boltBuilder;
	public SpoutBuilder spoutBuilder;
	public static final String SPOUT_STREAM = "spout-stream";
	public static final String MYSQL_STREAM = "mysql-stream";
	public static final String MONGODB_STREAM = "mongodb-stream";
	

	public Topology(InputStream configFile) throws Exception {
		this.configs = new Properties();
		try{
			configs.load(configFile);
			this.boltBuilder = new BoltBuilder(configs);
			this.spoutBuilder = new SpoutBuilder(configs);
		}catch(Exception ex){
			ex.printStackTrace();
			System.exit(0);
		}
	}

	private void submitTopology() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();	
		KafkaSpout kafkaSpout = spoutBuilder.buildKafkaSpout();
		GroupBolt groupBolt = boltBuilder.builderGroupBolt();
		MongodbBolt mongoBolt = boltBuilder.buildMongodbBolt();
		MysqlBolt mysqlBolt = boltBuilder.buildMysqlBolt();
		
		int kafkaSpoutCount = Integer.parseInt(configs.getProperty(Keys.KAFKA_SPOUT_CONT));
		builder.setSpout(configs.getProperty(Keys.KAFKA_SPOUT_ID), kafkaSpout, kafkaSpoutCount);
		
		int groupCount = Integer.parseInt(configs.getProperty(Keys.GROUP_TYPE_COUNT));
		//builder.setBolt(configs.getProperty(Keys.GROUP_TYPE_ID), groupBolt, groupCount).shuffleGrouping(configs.getProperty(Keys.KAFKA_SPOUT_ID), SPOUT_STREAM);
		builder.setBolt(configs.getProperty(Keys.GROUP_TYPE_ID), groupBolt, groupCount).shuffleGrouping(configs.getProperty(Keys.KAFKA_SPOUT_ID));
		
		int mongoBoltCount = Integer.parseInt(configs.getProperty(Keys.MONGO_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.MONGO_BOLT_ID),mongoBolt,mongoBoltCount).shuffleGrouping(configs.getProperty(Keys.GROUP_TYPE_ID), MONGODB_STREAM);
		
		int mysqlBoltCount = Integer.parseInt(configs.getProperty(Keys.MYSQL_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.MYSQL_BOLT_ID), mysqlBolt, mysqlBoltCount).shuffleGrouping(configs.getProperty(Keys.GROUP_TYPE_ID), MYSQL_STREAM);
				
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(1);
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.parseInt(configs.getProperty(Keys.TOPOLOGY_SEC)));
		conf.put(Config.NIMBUS_HOST, configs.getProperty(Keys.STORM_NIMBUS_HOST));
		conf.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(configs.getProperty(Keys.STORM_NIMBUS_PORT)));
		conf.put(Config.STORM_ZOOKEEPER_PORT, Integer.parseInt(configs.getProperty(Keys.KAFKA_ZOOKEEPER_PORT)));
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(configs.getProperty(Keys.KAFKA_ZOOKEEPER_HOST)));
		
		String topologyName = configs.getProperty(Keys.TOPOLOGY_NAME);
		
		StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		InputStream configFile;
		if (args.length == 0) {
			System.out.println("Missing input : config file location, using default");
			configFile = Topology.class.getResourceAsStream("/default_config.properties");			
		} else{
			configFile = new FileInputStream(args[0]);
		}
		
		Topology ingestionTopology = new Topology(configFile);
		ingestionTopology.submitTopology();
		
	}

}
