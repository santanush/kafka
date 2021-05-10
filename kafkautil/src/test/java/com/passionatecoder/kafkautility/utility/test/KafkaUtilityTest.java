package com.passionatecoder.kafkautility.utility.test;

import java.util.Set;

import com.passionatecoder.kafkautil.utility.KafkaUtility;
import com.passionatecoder.kafkautil.utility.Utility;

public class KafkaUtilityTest {
	public static void main(String[] args) throws Exception {
		
		//Step-1 Get the current topics descriptions
		Set<String> topics = KafkaUtility.getAllTopics();
		System.out.println("Current topics: " +Utility.getObjeDetailAsString(topics));
		
		//Step-2 Create first topic 
		KafkaUtility.createTopic("test-topic-1", 5, 1);
		
		//Step-1 Get the current topics descriptions
		topics = KafkaUtility.getAllTopics();
		System.out.println("Current topics: " +Utility.getObjeDetailAsString(topics));
		
	}

}
