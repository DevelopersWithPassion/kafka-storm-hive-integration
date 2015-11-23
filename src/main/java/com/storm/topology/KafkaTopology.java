package com.storm.topology;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaTopology {
	private static String zkhost, inputTopic, outputTopic, KafkaBroker, consumerGroup;
	private static String metaStoreURI, dbName, tblName;
	private static final Logger logger = Logger.getLogger(KafkaTopology.class);

	public static void Intialize(String arg) {
		Properties prop = new Properties();
		InputStream input = null;

		try {
			logger.info("Loading Configuration File for setting up input");
			input = new FileInputStream(arg);
			prop.load(input);
			zkhost = prop.getProperty("zkhost");
			inputTopic = prop.getProperty("inputTopic");
			outputTopic = prop.getProperty("outputTopic");
			KafkaBroker = prop.getProperty("KafkaBroker");
			consumerGroup = prop.getProperty("consumerGroup");
			metaStoreURI = prop.getProperty("metaStoreURI");
			dbName = prop.getProperty("dbName");
			tblName = prop.getProperty("tblName");

		} catch (IOException ex) {
			logger.error("Error While loading configuration file" + ex);

		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error("Error Closing input stream");

				}
			}
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		Intialize(args[0]);
		logger.info("Successfully loaded Configuration ");

		/*
		 * ZkHosts zkHosts = new ZkHosts(zkhost); SpoutConfig kafkaConfig = new
		 * SpoutConfig(zkHosts, inputTopic, "/" + inputTopic, consumerGroup);
		 * kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		 */

		BrokerHosts hosts = new ZkHosts(zkhost);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, inputTopic, "/" + KafkaBroker, consumerGroup);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.forceFromStart = false;
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		String[] partNames = { "col1" };
		String[] colNames = { "col2","col3","col4"};
		DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper().withColumnFields(new Fields(colNames))
				.withPartitionFields(new Fields(partNames));

		HiveOptions hiveOptions;
		//make sure you change batch size and all paramtere according to requirement
		hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper).withTxnsPerBatch(250).withBatchSize(2)
				.withIdleTimeout(10).withCallTimeout(10000000);

		logger.info("Creating Storm Topology");
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("KafkaSpout", kafkaSpout, 1);
		builder.setBolt("KafkaOutputBolt",
				new KafkaOutputBolt(zkhost, "kafka.serializer.StringEncoder", KafkaBroker, outputTopic), 1)
				.shuffleGrouping("KafkaSpout");

		builder.setBolt("HiveOutputBolt", new HiveOutputBolt(), 1).shuffleGrouping("KafkaSpout");
		builder.setBolt("HiveBolt", new HiveBolt(hiveOptions)).shuffleGrouping("HiveOutputBolt");

		Config conf = new Config();
		if (args != null && args.length > 1) {
			conf.setNumWorkers(3);
			logger.info("Submiting  topology to storm cluster");

			StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
		} else {
			// Cap the maximum number of executors that can be spawned
			// for a component to 3
			conf.setMaxTaskParallelism(3);
			// LocalCluster is used to run locally
			LocalCluster cluster = new LocalCluster();
			logger.info("Submiting  topology to local cluster");
			cluster.submitTopology("KafkaLocal", conf, builder.createTopology());
			// sleep
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.error("ERROR IS given" + e);

				logger.info("Killing Kafka topology");
				cluster.killTopology("KafkaToplogy");
				logger.info("Shutting down cluster");
				cluster.shutdown();
			}

			/*
			 * logger.info("Killing Kafka topology");
			 * cluster.killTopology("KafkaToplogy"); logger.info(
			 * "Shutting down cluster");
			 */
			cluster.shutdown();

		}

	}
}
