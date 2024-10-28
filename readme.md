# Creating the updated README.md file with the new section

# e2e Reddit Data Pipeline for Data Lakehouse

## Project Overview

This project demonstrates an end-to-end data pipeline that integrates Reddit data into a data lakehouse architecture using multiple tools and technologies.

### Pipeline Workflow:

1. **Extract**: Retrieve data from Reddit using Python.
2. **Publish**: Schedule and send data to Kafka topics.
3. **Transform**: Use Apache NiFi to consume and modify the data.
4. **Load**: Store transformed data in OpenSearch for searchability.
5. **Store**: Compress the data and upload it to S3, creating a data lakehouse.

---

## Need for This README

This README serves as a guide for setting up, troubleshooting, and understanding the architecture of the Reddit data pipeline project. Whether you're a new contributor, a data engineer learning about lakehouse pipelines, or someone debugging an existing setup, this document provides the essential information you need:

- **Purpose of the Project**: Overview of what the project does.
- **Setup Instructions**: Step-by-step commands to deploy the pipeline using Docker and Python.
- **Troubleshooting Tips**: Common fixes for permission issues.
- **Required Tools and Credentials**: A checklist to ensure you have the necessary software and API keys.
- **Contribution Guidelines**: Instructions for those looking to enhance the project.

This section ensures that anyone interacting with the project can get started easily and understand the context and purpose of each step in the pipeline.

---

## Getting Started

### Prerequisites

Make sure you have the following installed:

- **Docker & Docker Compose**
- **Python 3.12**
- **Reddit API Credentials**

---

## Setup

1. Clone the repository and navigate into the `airflow` directory:
   ```sh
   cd airflow
   ```

## Articles

[medium1](https://burakugur.medium.com/810eebc47757?source=friends_link&sk=b9bdf94c3d3c2a9c5380fcf85460d82c)

[medium12](https://burakugur.medium.com/07dd18b804d4?source=friends_link&sk=03718d7f676dbcdea846b47baee07be2)

## Licances

[MIT](https://choosealicense.com/licenses/mit/)

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)

## Troubleshooting

    ```sh
    sudo chown -R 1001:1001 ./data/kafka
    sudo chown -R 1001:1001 ./nifi_registry
    ```
