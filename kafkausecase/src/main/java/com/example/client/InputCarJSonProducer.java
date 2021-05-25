package com.example.client;

import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.utility.KafkaUtility;
import com.example.utility.PropertiesUtil;

public class InputCarJSonProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(InputCarJSonProducer.class);
	public void sendDeliveryMessage(String key,List<String> messages, String topic) throws Exception{		 
		KafkaProducer<String, String> producer = KafkaUtility.createProducer("car-info-producer", StringSerializer.class, StringSerializer.class);
		for(String record : messages) {
			LOGGER.info("Message is :"+record);
			producer.send(new ProducerRecord<String, String>(topic, key, record), new Callback() {
		          @Override
		          public void onCompletion(RecordMetadata m, Exception e) {
		        	  LOGGER.info("Message sent");
		            if (e != null) {
		              LOGGER.error("Error occurred",e);
		            } else {
		            	LOGGER.info("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
		            }
		          }
		      });
		}
		producer.flush();
		producer.close();
	}
}
