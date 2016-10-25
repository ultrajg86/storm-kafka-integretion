package com.imi.storm.mongodb;

import java.io.Serializable;

import org.apache.storm.tuple.ITuple;
import org.bson.Document;

public interface MongoMapper extends Serializable {
	Document toDocument(ITuple tuple);
}
