package com.example.test.utility;

import java.io.IOException;

import com.example.utility.KafkaUtility;
import com.example.utility.PropertiesUtil;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class KafkaUtilityTest {
	public static void main(String[] args) throws JsonGenerationException, JsonMappingException, IOException {
		//System.out.println("**************" +PropertiesUtil.getDQTopic());
		KafkaUtility.getAllTopics();
	
		KafkaUtility.createTopic(PropertiesUtil.getInputTopic(),5,1);
		KafkaUtility.printTopicDescription();
		
	}
}
