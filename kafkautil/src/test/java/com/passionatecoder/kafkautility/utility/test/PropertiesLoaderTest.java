package com.passionatecoder.kafkautility.utility.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.passionatecoder.kafkautil.utility.PropertiesLoader;

public class PropertiesLoaderTest {
	public static void main(String[] args) throws JsonProcessingException {
		PropertiesLoader propLoader = new PropertiesLoader();
		System.out.println(new ObjectMapper().writeValueAsString(propLoader.getAllPropertyNames()));
	}
}
