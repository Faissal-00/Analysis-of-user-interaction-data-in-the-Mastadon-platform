# Analysis of user interaction data in the Mastadon platform

## Table of Contents
- [Project Overview](#project-overview)
- [Pipeline Structure](#pipeline-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
- [Usage](#usage)
  - [Collecting Data](#collecting-data)
  - [MapReduce Processing](#mapreduce-processing)
  - [Storing Results in HBase](#storing-results-in-hbase)
  - [Orchestrating with Apache Airflow](#orchestrating-with-apache-airflow)
  - [Analyzing Results](#analyzing-results)
- [Optimization and Monitoring](#optimization-and-monitoring)
- [Data Access and Permissions](#data-access-and-permissions)
- [GDPR Compliance](#gdpr-compliance)
- [Contributing](#contributing)

## Project Overview

This project automates data collection, processing, and analysis using a data pipeline. It leverages various technologies to gather and analyze data from the Mastodon platform.

## Pipeline Structure

The pipeline consists of several components:
- Data Collection from Mastodon using its API.
- Storing raw data in HDFS for scalability.
- MapReduce processing to extract key metrics.
- Storing results in HBase for efficient querying.
- Orchestration using Apache Airflow.
- GDPR compliance for handling personal data.

## Getting Started

### Prerequisites

- [Python, Hadoop, HBase, Airflow and Mastodon API access.]


## Usage

### Collecting Data

To collect data from Mastodon, use the provided API access tokens.

### MapReduce Processing

MapReduce is used to process data and generate key metrics like user engagement, URL sharing, and emojis.

### Storing Results in HBase

Results are stored in HBase tables designed to efficiently support your analysis needs.

### Orchestrating with Apache Airflow

An Apache Airflow Directed Acyclic Graph (DAG) orchestrates the workflow, ensuring data pipeline tasks are executed seamlessly.

### Analyzing Results

Use SQL queries to analyze data and derive insights, such as identifying top users by followers or analyzing user engagement over time.

## Optimization and Monitoring

The project includes optimization of MapReduce scripts for better performance and monitoring tools to detect issues in the data pipeline.

## Data Access and Permissions

Ensure API tokens have the necessary permissions for data access. Document access rules and how to request access.

## GDPR Compliance

The project complies with GDPR regulations by documenting personal data handling and ensuring that all data processing activities meet GDPR requirements.

## Contributing

Contributions to this project are welcome. Feel free to report issues or submit pull requests.
