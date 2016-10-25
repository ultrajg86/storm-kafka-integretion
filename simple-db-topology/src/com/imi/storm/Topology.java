package com.imi.storm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

import com.imi.storm.bolt.BoltBuilder;
import com.imi.storm.bolt.MongodbBolt;
import com.imi.storm.spout.SpoutBuilder;

public class Topology {
	
	public Properties configs;
	public BoltBuilder boltBuilder;
	public SpoutBuilder spoutBuilder;
	public static final String MSG_STREAM = "msg-stream";
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
		//SinkTypeBolt sinkTypeBolt = boltBuilder.buildSinkTypeBolt();
		//SolrBolt solrBolt = boltBuilder.buildSolrBolt();
		//HdfsBolt hdfsBolt = boltBuilder.buildHdfsBolt();
		MongodbBolt mongoBolt = boltBuilder.buildMongodbBolt();
		
		
		//set the kafkaSpout to topology
		//parallelism-hint for kafkaSpout - defines number of executors/threads to be spawn per container
		int kafkaSpoutCount = Integer.parseInt(configs.getProperty(Keys.KAFKA_SPOUT_CONT));
		builder.setSpout(configs.getProperty(Keys.KAFKA_SPOUT_ID), kafkaSpout, kafkaSpoutCount);
		
		//set the mongodb bolt
		int mongoBoltCount = Integer.parseInt(configs.getProperty(Keys.MONGO_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.MONGO_BOLT_ID),mongoBolt,mongoBoltCount).shuffleGrouping(configs.getProperty(Keys.KAFKA_SPOUT_ID));
				
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(1);
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);		
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

