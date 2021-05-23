package com.example.repository;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.stream.config.JobConfig;
import com.example.utility.DatabaseUtility;

public class CarTrackingRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(CarTrackingRepository.class);
	private static CarTrackingRepository instance = null;
	private static String dataExists = "select count(*) from carstate.\"CAR_MOVEMENT_COUNT\" where movement_date=? and state_type=?";
	private static String dataInsert = "insert into carstate.\"CAR_MOVEMENT_COUNT\"(movement_date, created_on,created_by,state_type,state_count) values(?,?,?,?,?)";
	private static String dataUpdate = "update carstate.\"CAR_MOVEMENT_COUNT\" set state_count=?, created_on=?, created_by=? where movement_date=? and state_type=?";
	private static PreparedStatement pStmtExists  = null;
	private static PreparedStatement pStmtInsert  = null;
	private static PreparedStatement pStmtUpdate  = null;
	public static CarTrackingRepository getInstance()  {
		if(null == instance) {
			try {
				instance = new CarTrackingRepository();
				pStmtExists  = DatabaseUtility.getConnection().prepareStatement(dataExists);
				pStmtInsert  = DatabaseUtility.getConnection().prepareStatement(dataInsert);
				pStmtUpdate  = DatabaseUtility.getConnection().prepareStatement(dataUpdate);
			}catch(Exception ex) {
				LOGGER.error("Error creating instance",ex);
			}
		}
		return instance;
	}
	
	public Boolean populateDailyCarType(Map<String,Integer> map) {
		try {
			
			Set<String> keys = map.keySet();
			for(String key:keys) {
	              LOGGER.info("-------------------CAR DETAILS***************[" + key + ", " + map.get(key) + "]-------");
	              boolean doesExist = entryAlredyExists(pStmtExists, key,map.get(key));
	             if(doesExist) {
	            	
	            	 updateEntryForToday(pStmtUpdate,key,map.get(key));
	             }else {
	            	 insertEntryForToday(pStmtInsert,key,map.get(key));
	             }
	        
			}
		} catch (Exception e) {
			LOGGER.error("Error populating count dataabase",e);
		}
         
		 return true;
	}

	private void insertEntryForToday(PreparedStatement pStmtInsert, String key, Integer value) throws SQLException {
		LOGGER.info("Inserting the new value..............");
		
		pStmtInsert.setInt(5,value);
		pStmtInsert.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
		pStmtInsert.setString(3,"system");
		pStmtInsert.setObject(1, java.time.LocalDate.now());
		pStmtInsert.setString(4, key.toLowerCase());
		pStmtInsert.execute();
	}

	private void updateEntryForToday(PreparedStatement pStmtUpdate, String key, Integer value) throws SQLException {
		LOGGER.info("Udating the existing value..............");
		pStmtUpdate.setInt(1,value);	
		pStmtUpdate.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
		pStmtUpdate.setString(3,"system");
		pStmtUpdate.setObject(4, java.time.LocalDate.now());
		pStmtUpdate.setString(5, key.toLowerCase());		
		pStmtUpdate.execute();
		
	}

	private boolean entryAlredyExists(PreparedStatement pStmtExists, String key, Integer value)
			throws SQLException {
		 boolean exists = false;
		 LOGGER.info("Current date string is:::"+java.time.LocalDate.now());
		 pStmtExists.setObject(1, java.time.LocalDate.now());
		 pStmtExists.setString(2, key.toLowerCase());
		
		 ResultSet rSet = pStmtExists.executeQuery();
		 while(rSet.next()) {
			 int dataCount = rSet.getInt(1);
			 LOGGER.info("Data with key: "+key + " exists and cuurent count is:"+dataCount);
			 if(dataCount>0) {
				 exists = true;
			 }
		 }
		 return exists;
	}
	
	public static void main(String[] args) throws SQLException {
		Map<String, Integer> map = new HashMap<>();
		map.put(JobConfig.STATE_TOTAL_SEDAN_COUNT, 15);
		map.put(JobConfig.STATE_TOTAL_HATCHBACK_COUNT, 11);
		
		//map.put("hatchback", 150);
		//map.put("sedan", 101);
		CarTrackingRepository.getInstance().populateDailyCarType(map);
		
	}
}
