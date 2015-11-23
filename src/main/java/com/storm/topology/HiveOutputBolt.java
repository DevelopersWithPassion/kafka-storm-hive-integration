package com.storm.topology;

import java.sql.Timestamp;
import java.util.Map;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class HiveOutputBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(HiveOutputBolt.class);
	OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String obj = (String) input.getValue(0);
		//write parsing logic here to get necessary fields that is to be emited 
		
		
		//emit number of column that you required in hive table in sequence
                  	value = new Values(col1,col2,col3,col4);
				_collector.emit(value);
				_collector.ack(input);

	}

	public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
		//hive column name in sequence
		ofDeclarer.declare(new Fields("col1","col2","col3","col4"));
	}
}
