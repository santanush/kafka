package com.example.utility;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaUtility {

	private static AdminClient client = null;
	
	private static AdminClient getAdminClient() {
		client=null;
		if(null == client) {
			client = AdminClient.create(getAdminConfig());
		}
		return client;
	}
	
	public static void createTopic(final String topic, final int partitions, final int replication) {
		final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
		try (final AdminClient adminClient = getAdminClient()) {
			adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
		} catch (final InterruptedException | ExecutionException e) {
			e.printStackTrace();
			// Ignore if TopicExistsException, which may be valid if topic exists
			if (!(e.getCause() instanceof TopicExistsException)) {
				throw new RuntimeException(e);
			}
		}
	}
	public static void deleteTopic(final String topic) {
		
		try (final AdminClient adminClient = getAdminClient()) {
			adminClient.deleteTopics(Arrays.asList(topic));
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	public static Set<String> getAllTopics() {
		Set<String> topicNames = null;
		try (final AdminClient adminClient = getAdminClient()) {
			 Map<String,TopicListing> topics = adminClient.listTopics().namesToListings().get();
			 if(null != topics) {
				topicNames = topics.keySet();
				for(String topic : topicNames) {
					TopicListing listing = topics.get(topic);
					System.out.println("Topic Name---->"+topic);
					System.out.println("Listing name : " +listing.name() + " isInternal : "+listing.isInternal());
				}
			 }
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return topicNames;
	}
	private static Map<String,Object> getAdminConfig(){
		Map<String,String> configurations = new PropertiesLoader().getpropertiesAsMap();
		System.out.println("Property name:"+configurations.get("bootstrap.servers"));
		Map<String, Object> conf = new HashMap<>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configurations.get("bootstrap.servers"));
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, configurations.get("create.timeout"));		
		return conf;
	}
	
	public static void printTopicDescription() {
	    Collection<TopicListing> listings;
	    // Create  an AdminClient using the properties initialized earlier
	    try (AdminClient client = getAdminClient()) {
	    	ListTopicsOptions options = new ListTopicsOptions();
	    	listings = client.listTopics(options).listings().get();
	    	List<String> topics = listings.stream().map(TopicListing::name).collect(Collectors.toList());
	    	DescribeTopicsResult result = client.describeTopics(topics);
		      result.values().forEach((key, value) -> {
		        try {
		          System.out.println(key + ": " + value.get());
		        } catch (InterruptedException e) {
		          Thread.currentThread().interrupt();
		        } catch (ExecutionException e) {
		         	e.printStackTrace();
		        }
		      });
	    } catch (InterruptedException e) {
	      Thread.currentThread().interrupt();
	    } catch (ExecutionException e) {
	     e.printStackTrace();
	    }
	}
	public  static <K,V> KafkaProducer<K, V> createProducer(String clientId,Class<K> keyClass, Class<V> valueClass,String partitioner){
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyClass.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueClass.getName());
       // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<K,V>(props);
	}
	public  static <K,V,T1,T2> KafkaProducer<K, V> createProducer(String clientId,Class<T1> keyClass, Class<T2> valueClass){
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyClass.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueClass.getName());       
        return new KafkaProducer<K,V>(props);
	}
	public  static <K,V,T1,T2> Consumer<K, V> createConsumer(String consumerGroupId,Class<T1> keyClass, Class<T2> valueClass,List<String> topics,String partitioner){
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyClass.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueClass.getName());
       // props.put(ConsumerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, PropertiesUtil.getPropertyValue("consumer.enable.auto.commit"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, PropertiesUtil.getPropertyValue("consumer.max.poll.records"));
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);
        
        Consumer<K, V> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        return consumer;
	}
	public  static <K,V,T1,T2> Consumer<K, V> createConsumer(String consumerGroupId,Class<T1> keyClass, Class<T2> valueClass,List<String> topics){
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyClass.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueClass.getName());
        
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, PropertiesUtil.getPropertyValue("consumer.max.poll.records"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, PropertiesUtil.getPropertyValue("consumer.enable.auto.commit"));
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);
        
        Consumer<K, V> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        return consumer;
	}
	public static Consumer<String,String> createStatsConsumer(String groupId){
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.capgemini.aiengg.dq.serde.JsonDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer =
                new KafkaConsumer<String,String>(props);
        return consumer;
	}
	public static <K,V,T1,T2> KafkaProducer<K, V> createPssMessageProducer(String clientId, Class<T1> keyClass,
			Class<T2> valueClass) {
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getKafkaBroker());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyClass.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueClass.getName());       
        return new KafkaProducer<K,V>(props);
	}


	
}
