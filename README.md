# Happiness Score Prediction | CatBoost & Kafka

**A regression workflow from historical data exploration to streamed predictions.**

Academic machine learning project using annual happiness data from **2015–2019**. It combines feature harmonization, exploratory analysis, regression model comparison and a Kafka producer/consumer demonstration that stores predictions in PostgreSQL.

**[Explore the data](notebooks/001_EDA.ipynb)** · [Review model training](notebooks/002_ML.ipynb) · [Inspect the streaming workflow](streaming/feature_selection.py)

## Result and evaluation context

The project reports **CatBoost Regressor with R² = 0.86** on its **20% test split**. This is an internal experiment on historical observations. It is not a future-year forecast evaluation, and the streaming stage replays historical records rather than consuming a live public data feed.

For interpreting the result, distinguish the regression experiment from a temporal backtest: repeated countries across years may affect how well a random split represents future deployment. Temporal or country-grouped evaluation is an extension, not a result claimed here.

## What you can review

- **Data preparation:** harmonization of changing column names across five annual datasets.
- **Modeling:** exploratory analysis, common feature selection and regression comparison.
- **Delivery:** a Kafka producer sends feature records; the consumer loads the model, predicts a score and persists the result in PostgreSQL.
- **Engineering:** containerized Kafka services and separate producer/consumer scripts.

**Stack:** Python · pandas · scikit-learn · CatBoost · Apache Kafka · PostgreSQL · Docker.

---

## Implementation guide

This project aims to train a regression model to predict the happiness score of different countries using historical data, with data streamed via Kafka. The pipeline includes exploratory data analysis (EDA), feature engineering, model training, simulated streaming of historical data, prediction, and storing the results in a database.


## 🧪 Technologies Used

- Python
- Jupyter Notebook
- Apache Kafka
- Scikit-learn
- PostgreSQL


## 📁 Structure
```text
Workshop_003/
├── data/           # Annual CSV inputs and prepared data
├── models/         # Serialized trained model
├── notebooks/      # EDA and model training
├── streaming/      # Feature preparation, producer and consumer
├── docker-compose.yml
├── requirements.txt
└── README.md
```


## 🔍 Exploratory Data Analysis (EDA)

We analyzed 5 CSV files from different years. Common features were identified and inconsistencies across datasets were handled to ensure uniformity in feature selection.


## ⚙️ ETL and Modeling Process

- Load and clean data.

- Select consistent features across all datasets.

- Split data into 80% training and 20% testing.

- Train a regression model using scikit-learn.

- Serialize the trained model to a .pkl file.


## 🔄 Data Streaming with Kafka

- **_Kafka Producer_**: Streams transformed feature data.

- **_Kafka Consumer_**: Receives the data, uses the trained model to predict the happiness score, and stores the result in a database.

The following information is stored:

- Features used for the prediction
- Testing data
- Predicted data (happiness score)


## 📊 Performance Metric

A regression performance metric (R²) was used to evaluate the model on the testing dataset. The best performing model was **_CatBoost Regressor_**, achieving an **_R²_** score of **_0.86_**.


## 🛠️ Setup Instructions

### Clone the repository

Execute the following command to clone the repository

```bash
git clone https://github.com/natam226/Workshop_003

```
> From this point on all processes are done in Visual Studio Code

### Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  #On Windows: venv\Scripts\activate
```

### Credentials
To make a connection to the database you must have the database credentials in a JSON file called credentials. So this file must be created in the project folder, with the following syntax:

```bash
{
    "dbname": "DB_NAME",
    "user": "DB_USER",
    "password": "DB_PASSWORD",
    "host": "DB_HOST",
    "port": "DB_PORT"    
}
```
Ensure this file is included in `.gitignore`.

### Installing the dependencies
The necessary dependencies are stored in a file named requirements.txt. To install the dependencies you can use the command
```bash
pip install -r requirements.txt
```


## 🚀 How to Run

### Initialize kafka

Open a terminal in Visual Studio Code and start docker
```bash
docker-compose up -d --build
```

From the repository root, run this command in a terminal to initialize the consumer
```bash
cd streaming
python kafka_consumer.py 
```

Open a new terminal at the repository root and run this command to initialize the producer
```bash
cd streaming
python feature_selection.py
```



