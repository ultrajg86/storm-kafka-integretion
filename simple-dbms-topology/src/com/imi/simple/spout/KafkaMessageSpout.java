package com.imi.simple.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaMessageSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private String contextJson;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.contextJson = context.toJSONString();
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		this.collector.emit(new Values(this.contextJson));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("kafka-message"));
	}

}
