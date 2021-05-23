package com.example.stream;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.stream.config.Car;
import com.example.stream.config.JobConfig;
import com.example.utility.PropertiesUtil;

public class StreamJobWithState {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamJobWithState.class);
	public void processMessage() throws Exception {
		 LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<Inside Job with state store>>>>>>>>>>>>");		
		 String key = "myKey";	
		 Properties props = new Properties();
	     props.put(StreamsConfig.APPLICATION_ID_CONFIG, JobConfig.JOB_IDENTIFIER_MYJOB1);
	     props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
	     props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	     props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	     //props.put("default.deserialization.exception.handler", DQDeserializationExceptionHandler.class);
	 
	     StreamsBuilder builder = new StreamsBuilder();
	     KStream<String, String> events = builder.stream(PropertiesUtil.getInputTopic());
	     
	     KStream<String, List<String>> mappedEvents = events.mapValues(value -> {
	    	 List<String> list = new ArrayList<>();
	    	 list.add(0,value);
	    	 list.add(1, key);
	    	 return list;
	     });
	     
	     StoreBuilder<KeyValueStore<String, Integer>> countStore = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(JobConfig.STATE_STORE), 
					Serdes.String(), Serdes.Integer());
	     builder.addStateStore(countStore);
	     LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<Inside Job with state store>>>>>>>>>>>>111111");
	     KStream<String, Car> transformedEvents  = events.transformValues(() -> new StateCountTransformer(), JobConfig.STATE_STORE);
	     
	    transformedEvents.foreach((x,y)->{
	    	LOGGER.info("CAR info :"+y.getType()+ ":" +y.getModel());
	    });
	     
	     KafkaStreams streams = new KafkaStreams(builder.build(), props);
	     streams.start();
	     Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	            LOGGER.info("Stopping Streams");
	            streams.cleanUp();
	        }));
	}
	
}
