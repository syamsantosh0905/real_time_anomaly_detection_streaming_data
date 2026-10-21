# Real-Time Anomaly Detection in Streaming Data

## Overview

This project demonstrates how continuously generated data can be processed through an asynchronous pipeline and checked for unusual patterns in real time.

Built using Python, the system simulates a streaming environment and uses statistical techniques such as Exponentially Weighted Moving Average (EWMA) and Z-Score analysis to identify potential anomalies. The focus is on understanding how streaming pipelines work and how lightweight statistical methods can be used for monitoring incoming data.

## How It Works

The system follows a simple pipeline-based workflow:

1. **Data Generation:** Generates data to simulate a continuous stream.
2. **Pipeline Processing:** Passes incoming data through processing stages using asynchronous execution.
3. **Anomaly Detection:** Uses EWMA and Z-Score analysis to identify values that deviate from expected patterns.
4. **Output and Logging:** Displays processed results and records detected anomalies for review.

The modular design separates the main processing tasks, making the pipeline easier to understand and extend.

## Features

- Asynchronous pipeline processing using Python's `asyncio`
- Simulated streaming data with controlled delays
- Statistical anomaly detection using EWMA and Z-Score
- Console-based output for observing processed data and anomaly results
- Anomaly logging for later inspection
- Modular structure that can be extended with additional processing stages

## Technologies Used

- **Language:** Python
- **Asynchronous Processing:** `asyncio`
- **Detection Techniques:** EWMA and Z-Score
- **Execution Environment:** Local terminal or command prompt

## Getting Started

### Prerequisites

- Python 3 installed
- Git (optional, for cloning the repository)

### Run the Project

1. Clone the repository:

   ```bash
   git clone https://github.com/syamsantosh0905/real_time_anomaly_detection_streaming_data.git
   ```

2. Open the project directory:

   ```bash
   cd real_time_anomaly_detection_streaming_data
   ```

3. Run the pipeline script:

   ```bash
   python asyncio/gb_async_pipeline.py
   ```

The program runs locally and displays the processing output in the terminal.

## Detection Approach

**Exponentially Weighted Moving Average (EWMA)** gives more weight to recent observations, helping the system track changes in incoming data.

**Z-Score analysis** measures how far a value is from a reference level in terms of standard deviations. Values that cross the configured threshold can be flagged as potential anomalies.

These techniques provide a lightweight, interpretable alternative to more complex machine learning models. This project is a demonstration of statistical anomaly detection rather than a trained machine learning system.

## Project Structure

```text
real_time_anomaly_detection_streaming_data/
├── pipeline/
├── README.md
└── ...
```

The pipeline code contains the processing logic. The exact file and folder structure may vary with the current repository contents.

## Scope and Future Improvements

This project is intended as a practical demonstration of asynchronous data processing and statistical anomaly detection. It uses simulated data rather than a production data feed.

Possible improvements include:
- Connecting real data sources such as application logs or system metrics
- Adding configurable detection thresholds
- Improving monitoring and visualization
- Evaluating detection behavior against labelled datasets
- Integrating external alerting or monitoring tools

## What I Learned

Through this project, I explored asynchronous programming in Python, pipeline-based data processing, and the use of statistical techniques to identify unusual patterns in streaming data.
