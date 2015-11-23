package com.storm.topology;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaOutputBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private Producer<String, String> producer;
	private String zkConnect, serializerClass, topic, brokerList;
	private static final Logger logger = Logger.getLogger(KafkaOutputBolt.class);
	private Map<String, String> valueMap = new HashMap<String, String>();
	private String dataToTopic = null;
	OutputCollector _collector;

	public KafkaOutputBolt(String zkConnect, String serializerClass, String brokerList, String topic) {
		this.zkConnect = zkConnect;
		this.serializerClass = serializerClass;
		this.topic = topic;
		this.brokerList = brokerList;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		logger.info("Intializing Properties");
		_collector = collector;
		Properties props = new Properties();
		props.put("zookeeper.connect", zkConnect);
		props.put("serializer.class", serializerClass);
		props.put("metadata.broker.list", brokerList);
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String obj = (String) input.getValue(0);
		//write processing logic here 
		
		
	//sending data to kafka topic
		KeyedMessage<String, String> dataValue = new KeyedMessage<String, String>(topic, dataToTopic);
		producer.send(dataValue);
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("null"));
	}
}
