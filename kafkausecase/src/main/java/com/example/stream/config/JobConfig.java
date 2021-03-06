package com.example.stream.config;

public interface JobConfig {
	
	public static final String JOB_IDENTIFIER_MYJOB1 = "job-with-state-store";
	public static final String STATE_STORE = "state-store";
	public static final String TOPIC_JOB_WITH_STATE_STORE = "input-topic1";
	public static final String STATE_TOTAL_RECORDS = "total_cars_passed";
	public static final String STATE_TOTAL_HATCHBACK_COUNT = "hatchback_count";
	public static final String STATE_TOTAL_SEDAN_COUNT = "sedan_count";
	
	public static final String REST_API_HOST = "localhost";
	public static final Integer REST_API_PORT= 4001;
	
	public static final String JOB_IDENTIFIER_STATE_QUERY = "job-with-state-query";
	public static final String STATE_STORE_QUERY = "state-store-query";
	public static final String TOPIC_JOB_WITH_STATE_QUERY = "input-topic-query";

}
