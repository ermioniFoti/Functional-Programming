# Functional Programming Analytics with Scala & Spark ⚡

A software project developed to perform Big Data analytics using Functional Programming principles on the Apache Spark platform. This repository contains implementations of data analysis pipelines written in Scala.

This project was developed as part of the INF424 Functional Programming, Analytics and Applications course at the Technical University of Crete (Spring, 2024).



## 📌 Project Overview

The objective of this project is to perform large-scale data analytics by leveraging the distributed computing capabilities of Apache Spark. The project is divided into two distinct application scenarios, each requiring specific Spark abstractions (**RDDs** and **Dataframes**) to process real-world datasets.

**Key Constraints:**
* All implementations are written strictly in **Scala**.
* Data is loaded from and stored to **HDFS**.
* Analytics are performed on **entire datasets** (no subsets).



## 🚀 Application Scenarios

### 1. Reuters News Stories Analysis (Spark RDDs)
This scenario focuses on analyzing unstructured/semi-structured text data to determine the relevance of specific terms to news categories.

* **Dataset:** Reuters RCV1-v2 (News stories, category assignments, and term vectors).
* **Metric:** **Jaccard Index**. The relevance between a Term ($T$) and a Category ($C$) is calculated as:
    $$J(T, C) = \frac{|DOC(T) \cap DOC(C)|}{|DOC(T) \cup DOC(C)|}$$
* **Methodology:**
    1.  **Load Data:** Parse category assignments (`rcv1-v2.topics.qrels`) and term vectors (`lyrl2004_vectors*.dat`).
    2.  **Calculate Sets:** Compute the number of documents containing specific terms $|DOC(T)|$ and documents belonging to specific categories $|DOC(C)|$.
    3.  **Intersection:** Calculate the intersection $|DOC(T) \cap DOC(C)|$.
    4.  **Output:** Generate a list of `<category name>;<stemid>;<JaccardIndex>` tuples.

### 2. Vessel Trajectory Analytics (Spark Dataframes)
This scenario performs spatiotemporal analysis on vessel tracking data collected from AIS stations in the Aegean Sea (Piraeus and Syros).


* **Dataset:** `nmea_aegean.logs` (Timestamp, Station ID, MMSI, Lat/Lon, Speed, Course, Heading, Status).
* **Technology:** Spark SQL / Dataframes.
* **Queries Implemented:**
    1.  **Traffic Volume:** Number of tracked vessel positions per station per day.
    2.  **Most Active Vessel:** The Vessel ID (MMSI) with the highest number of tracked positions.
    3.  **Speed Analysis:** Average Speed Over Ground (SOG) for vessels appearing in both Station 8006 and Station 10003 on the same day.
    4.  **Navigation Accuracy:** Average absolute difference between Heading and Course Over Ground (COG) per station.
    5.  **Status Distribution:** The Top-3 most frequent vessel statuses (e.g., "Underway using engine", "At anchor").



## 🛠️ Technical Specifications

* **Language:** Scala (Functional Programming).
* **Platform:** Apache Spark.
* **Storage:** HDFS (Hadoop Distributed File System).
* **Execution Environment:** SoftNet Cluster (TUC).
* **Data Structures:** * **RDDs** for Scenario 1 (Unstructured text processing).
    * **Dataframes** for Scenario 2 (Structured log processing).


## 📊 Performance & Execution Analysis

The project includes a detailed report analyzing the execution flow on the Spark Cluster, focusing on:
* **Job Decomposition:** Analysis of why Spark created specific numbers of jobs per scenario.
* **Stage Splitting:** Explanation of shuffle boundaries and stage creation.
* **Parallelism:** Evaluation of task distribution and degree of parallelism achieved.




## 📂 Project Structure

* `Scenario1_Reuters`: Scala code for RDD-based text analysis.
* `Scenario2_Vessels`: Scala code for Dataframe-based trajectory analytics.
* `Report`: PDF document containing logic description, complexity analysis, and Spark UI execution screenshots.
* `Output`: Sample result files for both scenarios.

---

**Developed for the School of Electrical and Computer Engineering, Technical University of Crete.**
