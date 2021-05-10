package com.passionatecoder.kafkautil.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);
	private static PropertiesLoader props = new PropertiesLoader();

	/*
	public static Properties getStreamProperties() {
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getProperty("application.id.config"));
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, PssMessageSerde.class);
		return properties;
	}
	*/
	public static String getPropertyValue(String property) {
		return props.getProperty(property);
	}

	public static String getKafkaBroker() {
		return props.getProperty("bootstrap.servers");
	}

	public static String getTest1Topic() {

		return props.getProperty("topic.test1");
	}
	public static String getTest2Topic() {

		return props.getProperty("topic.test2");
	}
	
	public static void main(String[] args) {
		System.out.println(getTest1Topic());
	}
}
