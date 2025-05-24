# Workshop 003: Happiness Score Prediction with Machine Learning and Kafka

This project aims to train a regression model to predict the happiness score of different countries using historical data, with data streamed via Kafka. The pipeline includes exploratory data analysis (EDA), feature engineering, model training, real-time data streaming, prediction, and storing the results in a database.


## 🧪 Technologies Used

- Python
- Jupyter Notebook
- Apache Kafka
- Scikit-learn
- PostgreSQL


## 📁 Structure
```
.
├── data/                    # CSV files with happiness data by year
├── models/                  # Trained model (.pkl file)
├── notebooks/               # Jupyter notebooks for EDA, modeling, and prediction
├── streaming/               # Kafka producer and consumer scripts
├── database/                # Scripts to insert predictions into the database
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

Run this command in a terminal to initialize the consumer
```bash
python3 kafka_consumer.py 
```

Open a new terminal and run this command to initialize the producer
```bash
python3 feature_selection.py
```



