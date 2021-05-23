# Overview
This project shows how to use Kafka Admin to perform common functionality in Kafka. 
Below are the common finction implementation for Apache Kafka in this module.
This module will be etensively used throughout other kafka projects for interacting with Apache Kafka using Java.

# Kafka Utility module (Contains the commpon implementaion for working with Kafka)
### com.example.utility.KafkaUtility
|   Method|Description   |
|---|---|
| crateTopic  |  This method is used to create a topic with required partitiona and replication in your Kafka cluster |   
|  deleteTopic | This method is used to delete a Topic  |   
|  getAllTopics| This method is used to get all the topics from your cluster  | 
|  printTopicDesription|  This method iis used to describe your topic with metadata information |
|  createProducer |  Generic method for creating a Kafka Producer |
|  reateConsumer| Generic method to create a Kafka consumer |