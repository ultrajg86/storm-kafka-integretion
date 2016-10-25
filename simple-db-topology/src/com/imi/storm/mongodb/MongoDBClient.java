package com.imi.storm.mongodb;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;

public class MongoDBClient {
	
	private MongoClient client;
	private MongoCollection<Document> collection;
	
	public MongoDBClient(String url, String collectionName){
		MongoClientURI uri = new MongoClientURI(url);
		this.client = new MongoClient(uri);
		MongoDatabase db = this.client.getDatabase(uri.getDatabase());
		this.collection = db.getCollection(collectionName);
	}
	
	public void insert(List<Document> document, boolean orderd){
		InsertManyOptions options = new InsertManyOptions();
		if(!orderd){		
			options.ordered(orderd);
		}
		this.collection.insertMany(document, options);
	}
	
	public void update(Bson filter, Bson update, boolean upsert){
		UpdateOptions options = new UpdateOptions();
		if(upsert){
			options.upsert(upsert);
		}
		this.collection.updateMany(filter, update, options);
	}
	
	public void close(){
		this.client.close();
	}
	
}
