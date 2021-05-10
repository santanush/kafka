package com.passionatecoder.kafkautil.utility;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PropertiesLoader {
	private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesLoader.class);
	private static Map<String,String> map = null;
	public PropertiesLoader(){
		//if(null == map)
			loadProperties();
	}
	
	private void loadProperties(){
		Properties prop = new Properties();
		InputStream input = null;
       
		try {
			input = this.getClass().getResourceAsStream("/kafkaConfig.properties");
			LOGGER.info("Input is :: "+input);
			// load a properties file
			prop.load(input);

			map = new HashMap<>();
			
			Enumeration<?> propertyName = prop.propertyNames();
			
			while(propertyName.hasMoreElements()){
				
				String name = (String) propertyName.nextElement();
				String value = prop.getProperty(name);
				map.put(name, value);
			}
			
			
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public String getProperty(String key) {
		return map.get(key);
	}
	public Set<String> getAllPropertyNames() {
		return map.keySet();
	}
	public Map<String,String> getpropertiesAsMap(){
		return map;
	}
	public static void main(String[] args) throws JsonProcessingException {
		PropertiesLoader propLoader = new PropertiesLoader();
		System.out.println(new ObjectMapper().writeValueAsString(propLoader.getAllPropertyNames()));
	}
}
