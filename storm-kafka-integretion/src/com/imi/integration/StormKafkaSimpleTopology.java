package com.imi.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StormKafkaSimpleTopology {

	public static void main(String[] args) throws Exception{
		
		String topology = "test";
		String zkUrl = "localhost:2181";        // zookeeper url 
        String brokerUrl = "localhost:9092";

        if (args.length > 3 || (args.length == 1 && args[0].matches("^-h|--help$"))) {
            System.out.println("Usage: TridentKafkaWordCount [kafka zookeeper url] [kafka broker url]");
            System.out.println("   E.g TridentKafkaWordCount [" + zkUrl + "]" + " [" + brokerUrl + "]");
            System.exit(1);
        } else if (args.length == 1) {
        	topology = args[0];
            //zkUrl = args[0];
        } else if (args.length == 2) {
        	topology = args[0];
            zkUrl = args[1];
            //brokerUrl = args[1];
        }else if(args.length == 3){
        	topology = args[0];
            zkUrl = args[1];
            brokerUrl = args[2];
        }
		
		System.out.println("Using zookeeper url : " + zkUrl + " / broker url : " + brokerUrl);
		
		ZkHosts hosts = new ZkHosts(zkUrl);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "test", "/test", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 1);
		builder.setBolt("cutbolt", new CutLogBolt(), 8).shuffleGrouping("spout");
		builder.setBolt("classifybolt", new ClassifyKeyBolt(), 8).fieldsGrouping("cutbolt",new Fields("key","doctype"));
		builder.setBolt("docbolt", new DoctypeCountBolt(), 8).fieldsGrouping("classifybolt",new Fields("subdoctype"));
		
		Config conf = new Config();
		conf.setDebug(true);
		List<String> nimbus_seeds = new ArrayList<String>();
		nimbus_seeds.add("nimbus url");

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(topology, conf, builder.createTopology());
		}
		else {

			//=============================
			//	local mode
			//=============================
//			LocalCluster cluster = new LocalCluster();
//			cluster.submitTopology("log-stat", conf, builder.createTopology());
//			Thread.sleep(10000);
//			cluster.shutdown();
			
			//=============================
			//	cluster mode
			//=============================
			conf.put(Config.NIMBUS_HOST, "nimbus url");
			conf.put(Config.STORM_LOCAL_DIR,"your storm local dir");
			conf.put(Config.NIMBUS_THRIFT_PORT,6627);
			conf.put(Config.STORM_ZOOKEEPER_PORT,2181);
			conf.put(Config.STORM_ZOOKEEPER_SERVERS,Arrays.asList(new String[] {"zookeeper url"}));
//			conf.setNumWorkers(20);
//			conf.setMaxSpoutPending(5000);
			StormSubmitter.submitTopology("onlytest", conf, builder.createTopology());

		}
	}

}
