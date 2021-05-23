# Overview
This project shows how to use Kafka State Store in Kafka Streaming. The project has a common utility module to work with common Apache Kafka functionality e.g. Topic Creation, Printing Topic description, creating consumer, creating producer, delete a topic etc. 
The Streaming example shows how to consume real-time json messages from a topic and use State Store to perform state specific implementation in the streaming job.
To implement multiple kafa stream use cases the project uses input messages as JSON. The messages are maintained within InputMessages.txt file. This file contains list of Cars that is passing through a road at a time. 
Please find below use case specific Kafka state store implementations below for individual use case.

# Kafka Utility module (Contains the common implementation for working with Apache Kafka)
### com.example.utility.KafkaUtility
|   Method|Description   |
|---|---|
| crateTopic  |  This method is used to create a topic with required partitiona and replication in your Kafka cluster |   
|  deleteTopic | This method is used to delete a Topic  |   
|  getAllTopics| This method is used to get all the topics from your cluster  | 
|  printTopicDesription|  This method iis used to describe your topic with metadata information |
|  createProducer |  Generic method for creating a Kafka Producer |
|  reateConsumer| Generic method to create a Kafka consumer |

### com.example.client.InputCarJSonProducer
This is the implementation of a Producer that sends the CAR informations from InputMessages.txt file to the topic example-input-topic1. This Kafka Producer uses KafkaUtility.createProducer() method.

### UseCase-1 Using Key-Value state store
It is assumed there is a utility that sends number of CARS passed through a specific area in a road along with the car information to an input topic. This streaming job checks the input topic (example-input-topic1) with incoming car informations as JSON messages. The job internally manages the total number of cars passed through this location in a Key-Value state store. 
this job also maintains the number of *hatchback* and *sedan* CARS passing through that location and periodically saves this information to a metdata database.
The car movement details are maintained in **CAR_MOVEMENT_COUNT** table. The table details is given in dbScripts.sql file.
The Job utilizes **punctuate()** method every 30 minutes and stores this information in a table within *Postgres* database. This database information is maintained within kafkaConfig.properties. 

    com.example.stream.StreamJobWithState

Further this streaming Job utilizes below class for transforming input JSON messages to CAR object and also performs the state store update and punctuation method

**Validate the output in state table**
![Image of Metadata car tracking table](https://github.com/santanush/kafka/blob/master/kafkausecase/CarMovementCount.JPG)

    
    

