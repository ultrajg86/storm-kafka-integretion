package com.imi.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.imi.log.CustomLog;

public class CutLogBolt extends BaseRichBolt {
	
	
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		
		CustomLog log = new CustomLog();
		
		String value = tuple.toString();
		
		log.info("[TEST INFO] TUPLE : " + tuple);
		log.info("[TEST INFO] VALUE : " + value);
		
		int index = value.indexOf(" ");
		if(index == -1){
			return ;
		}
		
		String type = value.substring(0, index);
		value = value.substring(index);
		
		this.collector.emit("kafka-stream", new Values(type, value));
		this.collector.ack(tuple);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("kafka-stream", new Fields("text", "content"));
	}

}
