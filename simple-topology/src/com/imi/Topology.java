package com.imi;

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
import com.imi.log.CustomLog;

public class Topology {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		CustomLog log = new CustomLog();
		
		String zkUrl = "";
		String zkRoot = "";
		String topology = "";
		String topic = "";
		String groupId = "";
		
		if(args.length != 5){
			System.out.println("Usage : [zookeeper url] [zookeeper root] [topology] [topic] [group_id]");
			System.exit(0);
		}
		
		zkUrl = args[0];
		zkRoot = args[1];
		topology = args[2];
		topic = args[3];
		groupId = args[4];
		
		
		log.info("[TEST INFO] " + zkUrl + "|" + zkRoot + "|" + topology + "|" + topic + "|" + groupId);
		log.info("[TEST INFO] START");
		
		//kafka spout config
		BrokerHosts hosts = new ZkHosts(zkUrl);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		log.info("[TEST INFO] kafka config finish");
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout", kafkaSpout, 1);
		
		builder.setBolt("cutbolt", new CutLogBolt(), 8).shuffleGrouping("kafka-spout");
		
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(1);
		StormSubmitter.submitTopology(topology, config, builder.createTopology());
		
	}

}
