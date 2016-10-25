package com.imi.storm.bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;

import com.imi.storm.mongodb.AbstractMongoBolt;
import com.imi.storm.mongodb.MongoMapper;

public class MongodbBolt extends AbstractMongoBolt  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;

    private MongoMapper mapper;

    private boolean ordered = true;  //default is ordered.

    private int batchSize;

    private BatchHelper batchHelper;

    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;
    
	public MongodbBolt(String url, String collectionName, MongoMapper mapper) {
		super(url, collectionName);
		// TODO Auto-generated constructor stub
		Validate.notNull(mapper, "MongoMapper can not be null");
        this.mapper = mapper;
	}
	
	public MongodbBolt(String url, String collectionName) {
		super(url, collectionName);
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		try{
            if(batchHelper.shouldHandle(tuple)){
                batchHelper.addBatch(tuple);
            }

            if(batchHelper.shouldFlush()) {
                flushTuples();
                batchHelper.ack();
            }
        } catch (Exception e) {
           batchHelper.fail(e);
        }
	}
	
	private void flushTuples(){
        List<Document> docs = new LinkedList<Document>();
        for (Tuple t : batchHelper.getBatchTuples()) {
            //Document doc = mapper.toDocument(t);
        	List l = t.getValues();
        	Document doc = Document.parse(l.get(0).toString());
            docs.add(doc);
        }
        mongoClient.insert(docs, ordered);
    }
	
	public MongodbBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }
	
	public MongodbBolt withOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public MongodbBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
    	// TODO Auto-generated method stub
    	return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	// TODO Auto-generated method stub
    	super.prepare(stormConf, context, collector);
    	this.batchHelper = new BatchHelper(batchSize, collector);
    }

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}
