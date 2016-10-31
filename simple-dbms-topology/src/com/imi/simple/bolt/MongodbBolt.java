package com.imi.simple.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.imi.simple.mongodb.MongoDBClient;

public class MongodbBolt extends BaseRichBolt {
	
	private static final Logger LOG = LoggerFactory.getLogger(MongodbBolt.class);
	
	private static final long serialVersionUID = 1L;
	private String url;
	private String collection;
	private boolean ordered = true;
	private String contextJson;
	
	private OutputCollector collector;
	private MongoDBClient mongoClient;
	
	public MongodbBolt(String url, String collection){
		this.url = url;
		this.collection = collection;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.contextJson = context.toString();
		this.mongoClient = new MongoDBClient(url, collection);
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String message = tuple.getStringByField("content");
		
		if(!message.isEmpty()){
			Document document = Document.parse(message);
			this.mongoClient.insertOne(document);
		}
		
		
		/*
		List<Document> docs = new LinkedList<Document>();
		Document document = Document.parse(tupleMsg);
		this.mongoClient.insertMany(docs, this.ordered);
		*/
		
		/*List<Document> docs = new LinkedList<Document>();
		List tupleValuesList = tuple.getValues();
		for(int i=0; i<tupleValuesList.size(); i++){
			Document document = Document.parse(tupleValuesList.get(i).toString());
			docs.add(document);
		}
		this.mongoClient.insert(docs, this.ordered);*/
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		//super.cleanup();
		this.mongoClient.close();
	}

}