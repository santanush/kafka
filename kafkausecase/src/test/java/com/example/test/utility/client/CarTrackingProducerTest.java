package com.example.test.utility.client;

import java.util.List;

import com.example.client.InputCarJSonProducer;
import com.example.utility.Utility;

public class CarTrackingProducerTest {
	public static void main(String[] args) throws Exception {
		InputCarJSonProducer producer = new InputCarJSonProducer();
		List<String> messages = Utility.getJsonValuesFromFile();
		try {
			System.out.println("1. Sending CAR message as JSON.......");
			producer.sendDeliveryMessage("delivery",messages);			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
