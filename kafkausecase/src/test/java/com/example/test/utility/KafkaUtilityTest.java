package com.example.test.utility;

import java.io.IOException;

import com.example.utility.KafkaUtility;
import com.example.utility.PropertiesUtil;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class KafkaUtilityTest {
	public static void main(String[] args) throws JsonGenerationException, JsonMappingException, IOException {
		//Call utility method to get all topics
		KafkaUtility.getAllTopics();
		//Call methods to create topic
		//KafkaUtility.createTopic(PropertiesUtil.getInputTopic(),5,1);
		KafkaUtility.createTopic(PropertiesUtil.getInputTopicForApi(), 4, 1);
		KafkaUtility.printTopicDescription();
		
	}
}
