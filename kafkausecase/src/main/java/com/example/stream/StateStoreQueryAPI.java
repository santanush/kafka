package com.example.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.stream.config.JobConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import spark.Spark;

public class StateStoreQueryAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(StateStoreQueryAPI.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaStreams streams;
    private final HostInfo hostInfo;
    private Client client;
    private Boolean isActive = false;
    private final String NOT_FOUND = "{\"code\": 204, \"message\": \"No data exists\"}";
    private final String SERVICE_UNAVAILABLE = "{\"code\": 503, \"message\": \"Service is currently unavailable at this moment\"}";

    StateStoreQueryAPI(KafkaStreams stream, String hostname, int port) {
        this.streams = stream;
        this.hostInfo = new HostInfo(hostname, port);
        client = ClientBuilder.newClient();
    }

    void setActive(Boolean state) {
        isActive = state;
    }

    private String getValue(String searchKey) throws JsonProcessingException {
        ReadOnlyKeyValueStore<String, Integer> departmentStore = streams.store(
            JobConfig.STATE_STORE_QUERY, QueryableStoreTypes.keyValueStore());
        Integer result = departmentStore.get(searchKey);

        return (result == null) ? NOT_FOUND : objectMapper.writeValueAsString(result);
    }

    private List<KeyValue<String, Integer>> getAllValues() {
        List<KeyValue<String, Integer>> results = new ArrayList<>();
        ReadOnlyKeyValueStore<String, Integer> departmentStore = streams.store(
        		JobConfig.STATE_STORE_QUERY, QueryableStoreTypes.keyValueStore());
        LOGGER.info("***********************"+departmentStore);
        departmentStore.all().forEachRemaining(results::add);
        return results;
    }

    private String getValueFromRemote(String searchKey, HostInfo hostInfo){
        String result;
        String targetHost = String.format("http://%s:%d/kv/%s", hostInfo.host(), hostInfo.port(), searchKey);

        result = client.target(targetHost).request(MediaType.APPLICATION_JSON).get(String.class);

        return result;
    }

    private List<KeyValue<String, Integer>> getAllValuesFromRemote(HostInfo hostInfo) throws IOException {
        List<KeyValue<String, Integer>> results = new ArrayList<>();
        String targetHost = String.format("http://%s:%d/dept/local", hostInfo.host(), hostInfo.port());

        String result = client.target(targetHost).request(MediaType.APPLICATION_JSON).get(String.class);

        if (!result.equals(NOT_FOUND)) {
            results = objectMapper.readValue(result, results.getClass());
        }
        return results;
    }

    void start() {
        LOGGER.info("Starting Car Count Query Server " + "http://" + hostInfo.host() + ":" + hostInfo.port());
        Spark.port(hostInfo.port());
        Spark.get("/carmovementtracker/bytype/:carType", (req, res) -> {
            String results;
            String carTypeKey = req.params(":carType");
            LOGGER.info("Request: /carmovementtracker/" + carTypeKey);
            if (!isActive) {
                results = SERVICE_UNAVAILABLE;
            }else {
                StreamsMetadata metadata = streams.metadataForKey(JobConfig.STATE_STORE_QUERY, carTypeKey, Serdes.String().serializer());
                if(metadata.hostInfo().equals(hostInfo)){
                    LOGGER.info("Retrieving key/value from Local...");
                    results = getValue(carTypeKey);
                }else {
                    LOGGER.info("Retrieving key/value from Remote...");
                    results = getValueFromRemote(carTypeKey, metadata.hostInfo());
                }
            }
            return results;
        });

        Spark.get("/carmovementtracker/all", (req, res) -> {
            List<KeyValue<String, Integer>> allResults = new ArrayList<>();
            String results;
            if (!isActive) {
                results = SERVICE_UNAVAILABLE;
            }else{
                Collection<StreamsMetadata> allMetaData = streams.allMetadataForStore(JobConfig.STATE_STORE_QUERY);
                for (StreamsMetadata metadata : allMetaData){
                    if(metadata.hostInfo().equals(hostInfo)){
                        LOGGER.info("Retrieving all key/value pairs from Local...");
                        allResults.addAll(getAllValues());
                    }else {
                        LOGGER.info("Retrieving all key/value pairs from Remote...");
                        allResults.addAll(getAllValuesFromRemote(metadata.hostInfo()));
                    }
                }
                results = (allResults.size() == 0) ? NOT_FOUND : objectMapper.writeValueAsString(allResults);
            }
            return results;
        });

        Spark.get("/carmovementtracker/local", (req, res) -> {
            List<KeyValue<String, Integer>> allResults;
            String results;
            if (!isActive) {
                results = SERVICE_UNAVAILABLE;
            }else{
                LOGGER.info("Retrieving all key/value pairs from Local...");
                allResults = getAllValues();
                results = (allResults.size() == 0) ? NOT_FOUND : objectMapper.writeValueAsString(allResults);
            }
            return  results;
        });
    }

    void stop() {
        client.close();
        Spark.stop();
    }
}
