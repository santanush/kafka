package com.passionatecoder.kafkautility.utility.test;

import com.passionatecoder.kafkautil.utility.KafkaUtility;

public class KafkaUtilityTest {
	public static void main(String[] args) {
		KafkaUtility utility = new KafkaUtility();
		//Step-1 Get the current topics descriptions
		utility.getAllTopics();
		
	}

}
