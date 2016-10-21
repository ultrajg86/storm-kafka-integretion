package com.imi;

import java.util.Arrays;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.imi.bolt.CutLogBolt;

public class Topology {
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		if(args.length != 5){
			System.out.println("Usage : [zookeeper] [nimbus] [topology] [topic] [group_id]");
			System.exit(0);
		}
		
		String zkUrl = args[0];
		String zkRoot = "/consumers";
		String nimbusHost = args[1];
		String topology = args[2];
		String topic = args[3];
		String groupId = args[4];
		
		//kafka spout config
		BrokerHosts hosts = new ZkHosts(zkUrl + ":2181");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout", kafkaSpout, 1);
		
		builder.setBolt("cutbolt", new CutLogBolt(), 8).shuffleGrouping("kafka-spout");
		
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(1);
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
		config.put(Config.NIMBUS_HOST, nimbusHost);
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkUrl));
		StormSubmitter.submitTopology(topology, config, builder.createTopology());
		
	}

}
