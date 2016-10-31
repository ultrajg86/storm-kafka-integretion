package com.imi.simple.bolt;

import java.util.Properties;

import com.imi.simple.Keys;

public class BoltBuilder {
	
	public Properties configs = null;
	
	public BoltBuilder(Properties configs){
		this.configs = configs;
	}
	
	public GroupBolt builderGroupBolt(){
		return new GroupBolt();
	}
	
	public MongodbBolt buildMongodbBolt(){
		String host = this.configs.getProperty(Keys.MONGO_HOST);
		int port = Integer.parseInt(this.configs.getProperty(Keys.MONGO_PORT));
		String db = this.configs.getProperty(Keys.MONGO_DATABASE);
		String collection = this.configs.getProperty(Keys.MONGO_COLLECTION);
		String url = host + ":" + port + "/" + db;
		
		return new MongodbBolt(url, collection);
	}
	
	public MysqlBolt buildMysqlBolt(){
		
		String host = this.configs.getProperty(Keys.MYSQL_BOLT_HOST);
		int port = Integer.parseInt(this.configs.getProperty(Keys.MYSQL_BOLT_PORT));
		String db = this.configs.getProperty(Keys.MYSQL_BOLT_DATABASE);
		String table = this.configs.getProperty(Keys.MYSQL_BOLT_TABLE);
		String user = this.configs.getProperty(Keys.MYSQL_DB_USER);
		String password = this.configs.getProperty(Keys.MYSQL_DB_PWD);
		String url = host + ":" + port + "/" + db;
		
		return new MysqlBolt(url, table, user, password);
	}
	
}
