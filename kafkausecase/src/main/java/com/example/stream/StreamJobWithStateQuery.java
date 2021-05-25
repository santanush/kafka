package com.example.stream;

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

public class StreamJobWithStateQuery {

	private static final Logger LOGGER = LoggerFactory.getLogger(StreamJobWithState.class);
	public void processMessage() throws Exception {
		 LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<Inside Job with state store Query>>>>>>>>>>>>");		
		 Properties props = new Properties();
	     props.put(StreamsConfig.APPLICATION_ID_CONFIG, JobConfig.JOB_IDENTIFIER_STATE_QUERY);
	     props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
	     props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	     props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	     props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, JobConfig.REST_API_HOST + ":" + JobConfig.REST_API_PORT);
	     //props.put("default.deserialization.exception.handler", DQDeserializationExceptionHandler.class);
	 
	     StreamsBuilder builder = new StreamsBuilder();
	     KStream<String, String> events = builder.stream(PropertiesUtil.getInputTopicForApi());
	   
	     StoreBuilder<KeyValueStore<String, Integer>> countStore = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(JobConfig.STATE_STORE_QUERY), 
					Serdes.String(), Serdes.Integer());
	     builder.addStateStore(countStore);
	     
	     KStream<String, Car> transformedEvents  = events.transformValues(() -> new StateQueryCountTransformer(), JobConfig.STATE_STORE_QUERY);
	     
	    transformedEvents.foreach((x,y)->{
	    	LOGGER.info("CAR info :"+y.getType()+ ":" +y.getModel());
	    });
	     //Start the kafka streaming
	     KafkaStreams streams = new KafkaStreams(builder.build(), props);
	     
	     StateStoreQueryAPI apiServer = new StateStoreQueryAPI(streams, JobConfig.REST_API_HOST, JobConfig.REST_API_PORT);
	        streams.setStateListener((currentState, prevState) -> {
	            LOGGER.info("State Changing to " + currentState + " from " + prevState);
	            apiServer.setActive(currentState == KafkaStreams.State.RUNNING && prevState == KafkaStreams.State.REBALANCING);
	        });
	     //Start the streams
	     streams.start();
	     //Start the api server
	     apiServer.start();
	     
	     Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	            LOGGER.info("Stopping Streams");
	            streams.cleanUp();
	        }));
	}
}
