# Database Replication using Apache Kafka

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  - [Install and Setup Kafka](#install-and-setup-kafka)
  - [Start Zookeeper](#start-zookeeper)
  - [Start Kafka Server](#start-kafka-server)
  - [Create a Kafka Topic](#create-a-kafka-topic)
  - [Enable SQL Server Change Tracking](#enable-sql-server-change-tracking)
- [Usage](#usage)

## Features
- Real-time replication between two databases.
- Uses Apache Kafka for change data capture.
- SQL Server Change Tracking enabled for efficient change detection.

## Prerequisites
- SQL Server with Change Tracking enabled.
- Apache Kafka and Zookeeper installed.

## Setup

### Install and Setup Kafka
Download and install Kafka from the [Apache Kafka website](https://kafka.apache.org/downloads).

### Start Zookeeper
Start Zookeeper using the following command:

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties.

###  Start Kafka Server
.\bin\windows\kafka-server-start.bat .\config\server.properties

### Create a Kafka Topic
Create a Kafka topic named test-topic:

.\bin\windows\kafka-topics.bat --create --topic test-topic --bootstr
### Start Kafka using this command:
.\bin\windows\kafka-server-start.bat .\config\server.properties
Create a Kafka Topic
Create a Kafka topic named test-topic:
.\bin\windows\kafka-topics.bat --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
### Enable SQL Server Change Tracking
Enable change tracking on your SQL Server
### Usage
Send data to Kafka using Kafka producers.
Receive data from Kafka consumers and apply changes to the second database.
