##Running Application on HDP 2.2

All the setting and configuration can be made from external properties file.proerty file will look like

			zkhost = localhost:2181
			inputTopic =locinput1
			outputTopic=locoutput
			KafkaBroker =sandbox.hortonworks.com:6667
			consumerGroup=id7
			metaStoreURI = thrift://sandbox.hortonworks.com:9083
			dbName = default
			tblName = location
			
			We can change it according to environment in which we will run application

	    1. Kafka Topic locinput1 and locoutput need to be created 

        2. Push json onto locinput1 topic (Json is under src/main/resources) folder

        3. Create Hive Table(Schema is under src/main/resources) folder

        5. Run Nimbus(Storm's master node)  by going into storm directory 

                bin/storm nimbus

        6. Run Supervisor(Storm's Worker node)  by going into storm directory  of hdp

                bin/storm supervisor

        7. Run jar that you get after packaging project  by going into storm directory of hdp
        
					1.To run on cluster mode we need to execute as 

				storm jar /home/ldap/chanchals/kafka-storm-integration-0.0.1-SNAPSHOT.jar com.storm.topology.KafkaTopology /home/ldap/chanchals/topology.properties TopologyName

					2. To run on local  mode we need to execute as 

				storm jar /home/ldap/chanchals/kafka-storm-integration-0.0.1-SNAPSHOT.jar com.storm.topology.KafkaTopology /home/ldap/chanchals/topology.properties 



        8. please execute following permision before running project 
            sudo su - hdfs -c "hdfs dfs -chmod 777 /tmp/hive"
            sudo chmod 777 /tmp/hive 
            
##How to Build Project Jar

 if you have Eclipse IDE or equivalent IDE then do following step 
 
	 			Import Project as Maven Project 
	 			Right Click on Project ->RunAs->Maven Build 
	 			now type clean package
	 			run your project
	 			jar file will be created under target directory 
 			
 			
 If you want to run it from command line
 
			simply go to project directory and do
			mvn clean package 
			
 			
            
 Project has been designed to use two storm bolts. one bolt will be used to push data into kafka topic outputhive and other bolt i.e HiveBolt is used to push data to hive table. Spout that i am using is kafkaSpout. please let me know if there is any issue while running it. # kafka-storm-hive-integration
