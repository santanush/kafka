package com.example.stream;

import java.io.IOException;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.stream.config.Car;
import com.example.stream.config.JobConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StateQueryCountTransformer implements ValueTransformer<String, Car>{

	private static final Logger LOGGER = LoggerFactory.getLogger(StateCountTransformer.class);
	private KeyValueStore<String, Integer> statesStore;
	private ObjectMapper mapper;
	@Override
	public void init(ProcessorContext context) {
		this.statesStore = (KeyValueStore<String, Integer>)context.getStateStore(JobConfig.STATE_STORE_QUERY);
		
	}

	@Override
	public Car transform(String value) {
		assignTotalCount();
		Car car = null;
		try {
			car = Car.create(value);
			if(null != car) {
				assignCarTypeCount(car.getType());
			}
		} catch (IOException e) {
			LOGGER.error("Error parsing car information", e);
		}
	
		return car;
	}

	private void assignCarTypeCount(String carType) {
		carType = carType.toLowerCase();
		Integer count = statesStore.get(carType);
		LOGGER.info("***************(((CURRENT)))Total COUNT For CarType >>>>>>>>>>>>>>>>"+count);
		if(null == count) {
			statesStore.put(carType, 1);			
		}else {
			count = count + 1;
			statesStore.put(carType, count);
		}		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	private int assignTotalCount() {
		Integer totalRecords = statesStore.get(JobConfig.STATE_TOTAL_RECORDS);
		LOGGER.info("***************(((CURRENT)))Total messaages processed >>>>>>>>>>>>>>>>"+totalRecords);
		if(null == totalRecords) {
			statesStore.put(JobConfig.STATE_TOTAL_RECORDS, 1);
			totalRecords = 1;
		}else {
			totalRecords = totalRecords + 1;
			statesStore.put(JobConfig.STATE_TOTAL_RECORDS, totalRecords);
		}	
		return totalRecords;
	}
}
