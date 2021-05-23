package com.example.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);
	private static PropertiesLoader props = new PropertiesLoader();

	
	public static String getPropertyValue(String property) {
		return props.getProperty(property);
	}

	public static String getKafkaBroker() {
		return props.getProperty("bootstrap.servers");
	}

	public static String getInputTopic() {

		return props.getProperty("topic.input");
	}
	public static String getOutputTopic() {
		return props.getProperty("topic.output");
	}
	public static String getErrorTopic() {
		return props.getProperty("topic.error");
	}
	
	
}
