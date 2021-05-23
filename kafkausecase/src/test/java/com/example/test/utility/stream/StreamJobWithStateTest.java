package com.example.test.utility.stream;

import com.example.stream.StreamJobWithState;

public class StreamJobWithStateTest {
	public static void main(String[] args) throws Exception {
		StreamJobWithState stateStoreJob = new StreamJobWithState();
		stateStoreJob.processMessage();
	}

}
