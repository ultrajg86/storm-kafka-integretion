package com.imi.storm.mongodb;

import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;

public abstract class AbstractMongoBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String url;
    private String collectionName;

    protected OutputCollector collector;
    protected MongoDBClient mongoClient;

    public AbstractMongoBolt(String url, String collectionName) {
       Validate.notEmpty(url, "url can not be blank or null");
       Validate.notEmpty(collectionName, "collectionName can not be blank or null");

       this.url = url;
       this.collectionName = collectionName;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mongoClient = new MongoDBClient(url, collectionName);
    }

    @Override
    public void cleanup() {
       this.mongoClient.close();
    }

}
