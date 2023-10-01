# ***Weather Stations Monitoring***

### Design Data-Intensive Apps Course Project
### Project Description
    The Internet of Things (IoT) is an important source of data streams in the modern digital world.
    The “Things” are huge in count and emit messages in very high frequency which flood the
    global internet. Hence, efficient stream processing is inevitable.

    One use case is the distributed weather stations use case. Each “weather station” emits
    readings for the current weather status to the “central base station” for persistence and
    analysis. In this project, you will find the implementation of the architecture of a weather
    monitoring system.

## Authors

> **Ahmed Aboeleid**
>
> **Mohamed Salama**
>
> **Youssef Bazina**

## Table of Content
- [Setup](#Setup)
    - [Local Setup](#Local-Setup)
    - [K8s Setup](#K8s-Setup)
- [System Architecture](#System-Architecture)
    1) [Data Acquisition](#Data-Acquisition)
    2) [Data Processing and Archiving](#Data-Processing-and-Archiving)
    3) [Data Indexing](#Data-Indexing)

## Setup

### ***Note This setup for Ubuntu***
- You need to install **[Java](https://www.oracle.com/eg/java/technologies/downloads/) (19 Minimum)**, **[Kafka](https://kafka.apache.org/downloads)**, **[Kafka Image](https://hub.docker.com/r/bitnami/kafka/)**, **[Docker](https://docs.docker.com/engine/install/)**, **[K8s](https://kubernetes.io/docs/setup/)**, and **[Elasticsearch Image](https://hub.docker.com/r/nshou/elasticsearch-kibana)**.

### Local Setup

1) Run Kafka Commands Respectively

   | Service                              | Command                                                                                                          |
   |--------------------------------------|------------------------------------------------------------------------------------------------------------------| 
   | Zoo Keeper                           | bin/zookeeper-server-start.sh config/zookeeper.properties                                                        |
   | Kafka Server                         | bin/kafka-server-start.sh config/server.properties                                                               |
   | Create Weather Status Messages Topic | bin/kafka-topics.sh --create --topic weather-status-messages --bootstrap-server localhost:9092                   |
   | Create Raining Status Messages Topic | bin/kafka-topics.sh --create --topic raining-status-messages --bootstrap-server localhost:9092                   |
   | Run Kafka Consumer                   | bin/kafka-console-consumer.sh --topic weather-status-messages --from-beginning --bootstrap-server localhost:9092 |

2) Run [Bitcask](./Base-Central-Station/src/main/java/bitCask).
3) Run [Base Central Station](./Base-Central-Station/src/main/java/baseCentralStation).
4) Run [Weather Station](./Data%20Acquisition/weatherStation). Run multiple instances with arguments `station_id latitude longitude` to simulate multiple stations, e.g. `1 30.0444 31.2357`.
5) Run [Kafka Processor](./Data%20Acquisition/kafkaProcessor).

#### ***Now you can see the messages in each terminal.***

### K8s Setup

#### ***Not implemented yet.***

## System Architecture
There are 3 major components implemented using 6 microservices.

The 3 major components

### Data Acquisition
Multiple Weather Stations which feed a message queueing service ***Kafka*** with their readings.

#### Weather Stations
- Implemented in [Weather Station](./Data%20Acquisition/weatherStation)
- Weather station gets its data from Open-Meteo API according to a latitude and longitude the API.
- Data is fetched every 1 second.
- The properties of this data is **battery distribution(30% low - 40% medium - 30% high)** and dropping percentage of **10%**.
- Built using **Adapter Integration Pattern** to connect our App to Open-Meteo and to receive data on the needed-form.

#### Kafka Processor
- Implemented in [Kafka Processor](./Data%20Acquisition/kafkaProcessor).
- There are two types of processing following

| Processing Type   | Description                                                                                                                                         |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| Dropping Messages | Processes messages by probabilistic sampling of **10%**, then throw some of them away                                                               |
| Raining Areas     | Processes messages and detects rain when **humidity > 70%**, then pass new messages to raining topic. Kafka Streams and Filters are used to do this |

- Kafka streams produce messages to **Weather Topic** And records which have **humidity > 70** (**Pipe & Filter Patterns**) go to **Raining Topic**.
- Built using **Envelope Wrapper** as each message Kafka streams unwraps it and processes it then wraps it again as raining status message.
- Dropped messages go to **Invalid Channel** which is a RocksDB.


### Data Processing and Archiving

- Data Processing and Archiving is implemented in [Base Central Station](./Base-Central-Station/src/main/java/baseCentralStation).
- It consumes messages from **Weather Topic** and **Raining Topic**.
- It then writes them to **Parquet Files**, Parquet writer **aggregates** every 10k records and flushes them to the file.
- Files are partitioned by **day** and **station_id**.
- When the writer shuts down, when it restarts it will create a new file for the same station if it's the same day with new version number.

### Data Indexing

1. [BitCask Storage](#bitcask-storage)
2. [Elasticsearch and Kibana](#elasticsearch-and-kibana)

#### Bitcask Storage
- Implemented in [bitCask](./Base-Central-Station/src/main/java/bitCask) With ***JavaDocs***.
- We implemented the BitCask Riak LSM to maintain an updated store of each station status as discussed in [This paper](https://drive.google.com/file/d/1lmbsRqa-Z8mJDCD2KAqIDJeGjMbkuZeo/view?usp=sharing)
    *  **Scheduled Compaction** over Replica Files to avoid disrupting active readers.
    *  **Tombstones** for deletions to mark deleted entries, so they are skipped at compaction process.
    *  **The Entry Structure** in active and replica files is as follows

       | **ENTRY** | timestamp | key size | value size | key      | value      |
       |-----------|-----------|----------|------------|----------|------------|
       | **SIZE**  | 8 bytes   | 4 bytes  | 4 bytes    | key size | value size |
    * **The Entry Structure** in hint files is as follows

       | **ENTRY** | timestamp | key size | value size | value position | key      |
       |-----------|-----------|----------|------------|----------------|----------|
       | **SIZE**  | 8 bytes   | 4 bytes  | 4 bytes    | 8 bytes        | key size |

    * #### Crash Recovery Mechanism
        - Create a new in-memory structure called **keydir**.
        - Reads hint files if found, from start to end, and fill keydir with key value pairs.
        - If hint file is not found for specific timestamp, it reads Active file, from start to end, and fill keydir with key value pairs.
    * #### Compaction Mechanism
        - Loop on all replica files, read each replica file from start to end, add its key value pairs to hashMap.
        - Loop on keydir, write each key value as entry in a compacted file.
        - Delete replica files.
    * #### MultiWriter concurrency Mechanism
        - One writer at a time, other writers wait until the lock is released.
        - Multiple readers can read at the same time.
      
    *  **No checksums implemented** to detect errors.


#### Elasticsearch and Kibana
- Implemented in [elastic-search-and-kibana](./Elastic%20Search%20and%20Kibana).
- A command is run to **loop on parquet files** in elasticsearch.
- Kibana's visualisations confirming Battery status distribution of some stations confirming the battery distribution of stations
  ![image](https://github.com/basel-bytes/Weather-Stations-Monitoring/assets/95547833/5adae40f-c1cf-41d9-b4b5-e62c031b20e2)

- Kibana's visualisations calculating the percentage of dropped messages from stations confirming the required percentage 10%
  ![image](https://github.com/basel-bytes/Weather-Stations-Monitoring/assets/95547833/86d3962b-6ca2-42a2-a430-531b725e5787)