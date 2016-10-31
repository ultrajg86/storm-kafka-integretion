package com.imi.simple.bolt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.jgrapht.util.ArrayUnenforcedSet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.imi.simple.mysql.MysqlDBClient;

public class MysqlBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String url;
	private String table;
	private String user;
	private String password;
	
	private OutputCollector collector;
	
	private MysqlDBClient mysqlClient;
	
	public MysqlBolt(String url, String table, String user, String password){
		this.url = url;
		this.table = table;
		this.user = user;
		this.password = password;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.mysqlClient = new MysqlDBClient(this.url, this.user, this.password);
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		try{
			
			
			//{"method":"GET","query":"","uri":"\/","date":"2016-10-27 04:17:47","word":"observer_58117feb0a3f6","count":7962}" 
			
			String message = tuple.getStringByField("content");
			
			if(!message.isEmpty()){
				Document document = Document.parse(message);
				Object[] values = new Object[]{
						"method='" + document.getString("method") + "'",
						"query='" + document.getString("query") + "'",
						"uri='" + document.getString("uri") + "'",
						"reg_date='" + document.getString("date") + "'",
						"word='" + document.getString("word") + "'",
						"count='" + document.getInteger("count") + "'"
					};
					
					this.mysqlClient.insert(this.table, values);
			}
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		/*try {			
			
			List l = tuple.getValues();
			
			for(int i=0; i<l.size(); i++){
				JSONParser jsonParser = new JSONParser();
				JSONObject json = (JSONObject) jsonParser.parse(l.get(i).toString());
				ArrayList<Object> values = new ArrayList<>();
				
				//fields
				Iterator<String> keys = json.keySet().iterator();
				while(keys.hasNext()){
					String key = keys.next();
					String value = (String) json.get(key);
					values.add(key + "='" + value + "'");
				}
				
				this.mysqlClient.insert(this.table, values.toArray());
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}
