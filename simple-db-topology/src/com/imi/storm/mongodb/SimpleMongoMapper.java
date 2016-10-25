package com.imi.storm.mongodb;

import org.apache.storm.tuple.ITuple;
import org.bson.Document;

public class SimpleMongoMapper implements MongoMapper {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String[] fileds;
	
	public SimpleMongoMapper withFields(String... fields){
		this.fileds = fields;
		return this;
	}

	public Document toDocument(ITuple tuple) {
		// TODO Auto-generated method stub
		Document document = new Document();
		for(String field : this.fileds){
			document.append(field, tuple.getValueByField(field));
		}
		return document;
	}

}
