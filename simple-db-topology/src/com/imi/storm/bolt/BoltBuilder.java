package com.imi.storm.bolt;

import java.util.Properties;

import com.imi.storm.Keys;
import com.imi.storm.mongodb.MongoMapper;
import com.imi.storm.mongodb.SimpleMongoMapper;

public class BoltBuilder {
	
	public Properties configs = null;
	
	public BoltBuilder(Properties configs){
		this.configs = configs;
	}
	
	public MongodbBolt buildMongodbBolt(){
		String host = this.configs.getProperty(Keys.MONGO_HOST);
		int port = Integer.parseInt(this.configs.getProperty(Keys.MONGO_PORT));
		String db = this.configs.getProperty(Keys.MONGO_DATABASE);
		String collection = this.configs.getProperty(Keys.MONGO_COLLECTION);
		String mongodbBoltId = this.configs.getProperty(Keys.MONGO_BOLT_ID);
		//return new MongodbBolt(host, port, db, collection, mongodbBoltId);
		String url = host + ":" + port + "/" + db;
		
		//MongoMapper mapper = new SimpleMongoMapper().withFields("word", "count");
		
		//return new MongodbBolt(url, collection, mapper);
		return new MongodbBolt(url, collection);
	}
}
