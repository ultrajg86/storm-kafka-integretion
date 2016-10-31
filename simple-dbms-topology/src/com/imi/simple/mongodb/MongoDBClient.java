package com.imi.simple.mongodb;

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
	
	public MongoDBClient(String url, String collection){
		MongoClientURI uri = new MongoClientURI(url);
		this.client = new MongoClient(uri);
		MongoDatabase db = this.client.getDatabase(uri.getDatabase());
		this.collection = db.getCollection(collection);
	}
	
	public void insertOne(Document document){
		this.collection.insertOne(document);
	}
	
	public void insertMany(List<Document> document, boolean ordered){
		InsertManyOptions options = new InsertManyOptions();
		if(!ordered){
			options.ordered(ordered);
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
