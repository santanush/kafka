package com.example.stream;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.repository.CarTrackingRepository;
import com.example.stream.config.Car;
import com.example.stream.config.JobConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class StateCountTransformer implements ValueTransformer<String, Car> {
	private static final Logger LOGGER = LoggerFactory.getLogger(StateCountTransformer.class);
	private KeyValueStore<String, Integer> statesStore;
	private ObjectMapper mapper;
	@Override
	public void init(ProcessorContext context) {
		this.statesStore = (KeyValueStore<String, Integer>)context.getStateStore(JobConfig.STATE_STORE);
		
		context.schedule(Duration.ofMinutes(30l), PunctuationType.WALL_CLOCK_TIME, (timestamp)->{
			Map<String,Integer> map = new HashMap();
			try (final KeyValueIterator<String, Integer> iter = statesStore.all()) {
                LOGGER.info("-------@@@@@@@@@@@@@@@@@@@@@@---- " + timestamp + " ------@@@@@@@@@@@@@@@@@@@@@@----- ");
                while (iter.hasNext()) {
                    KeyValue<String, Integer> entry = iter.next();
                    LOGGER.info("*******************"+entry.key+ ">>>>>>>>>>>>>>>>"+entry.value);
                    map.put(entry.key, entry.value);
                    //context.forward(entry.key, entry.value.toString());
                }
                iter.close();

                // commit the current processing progress
                context.commit();
                 
                //CompletableFuture.supplyAsync(() -> CarTrackingRepository.getInstance().populateDailyCarType(iter));
                CarTrackingRepository.getInstance().populateDailyCarType(map);
            
            }
		});
	}

	@Override
	public Car transform(String value) {
		assignTotalCount();
		Car car = null;
		try {
			car = Car.create(value);
			if(null != car) {
				if(car.getType().equalsIgnoreCase("hatchback")) {
					assignHatchBackCount();
				}
				if(car.getType().equalsIgnoreCase("sedan")) {
					assignSedanCount();
				}
			}
		} catch (IOException e) {
			LOGGER.error("Error parsing car information", e);
		}
		try {
			LOGGER.info("CAr returned:::::::::::::::::::::"+new ObjectMapper().writeValueAsString(car));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return car;
	}

	private void assignSedanCount() {
		Integer sedanCount = statesStore.get(JobConfig.STATE_TOTAL_HATCHBACK_COUNT);
		LOGGER.info("***************(((CURRENT)))Total HATCHBACK COUNT >>>>>>>>>>>>>>>>"+sedanCount);
		if(null == sedanCount) {
			statesStore.put(JobConfig.STATE_TOTAL_HATCHBACK_COUNT, 1);
			sedanCount = 1;
		}else {
			sedanCount = sedanCount + 1;
			statesStore.put(JobConfig.STATE_TOTAL_HATCHBACK_COUNT, sedanCount);
		}		
	}

	private void assignHatchBackCount() {
		Integer hatchbackCount = statesStore.get(JobConfig.STATE_TOTAL_SEDAN_COUNT);
		LOGGER.info("***************(((CURRENT)))Total HATCHBACK COUNT >>>>>>>>>>>>>>>>"+hatchbackCount);
		if(null == hatchbackCount) {
			statesStore.put(JobConfig.STATE_TOTAL_SEDAN_COUNT, 1);
			hatchbackCount = 1;
		}else {
			hatchbackCount = hatchbackCount + 1;
			statesStore.put(JobConfig.STATE_TOTAL_SEDAN_COUNT, hatchbackCount);
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
