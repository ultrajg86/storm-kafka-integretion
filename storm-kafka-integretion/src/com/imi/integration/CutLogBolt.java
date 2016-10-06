package com.imi.integration;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CutLogBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String[] splitArray = input.getString(0).split(";");
		String key = "";
		String doctype = "";
		for(int i = 0; i < splitArray.length; i++){
			if(splitArray[i].contains("key"))
				key  = splitArray[i];
			if(splitArray[i].contains("doctype"))
				doctype = splitArray[i];
		}
		collector.emit(new Values(key,doctype));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","doctype"));
	}

	
}
