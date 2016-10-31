package com.imi.simple.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imi.simple.Topology;

public class GroupBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final Logger LOG = LoggerFactory.getLogger(GroupBolt.class);
	
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	
	private String jsonString;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.jsonString = context.toJSONString();
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		List<Object> list = new ArrayList<>();
		
		Object message = tuple.getValue(0);
				
		//mongodb
		//this.collector.emit(Topology.MONGODB_STREAM, new Values(this.jsonString));	
		this.collector.emit(Topology.MONGODB_STREAM, tuple, new Values(message));
		
		//mysql
		//this.collector.emit(Topology.MYSQL_STREAM, new Values(message));
		this.collector.emit(Topology.MYSQL_STREAM, tuple, new Values(message));
						
		this.collector.ack(tuple);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream(Topology.MONGODB_STREAM, new Fields("content"));
		declarer.declareStream(Topology.MYSQL_STREAM, new Fields("content"));
	}

}
