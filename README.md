# Amazon Books Data Pipeline


Overview
This project implements a data pipeline using Apache Airflow to scrape book data from Amazon, process it, and load it into a PostgreSQL database. It automates data extraction, transformation, and loading (ETL) tasks, demonstrating the power of orchestration tools and Python-based web scraping.

Features
Web Scraping

Collects details of books, including titles, authors, prices, and ratings, using Python libraries (requests and BeautifulSoup).
Removes duplicate entries and handles missing data.
Data Transformation

Cleans and processes the scraped data to ensure reliability and usability.
Database Operations

Automatically creates a PostgreSQL table to store book data.
Inserts the transformed data into the database.
Task Orchestration

Leverages Apache Airflow to schedule, monitor, and manage the ETL pipeline.
Utilizes Python and Postgres operators to create modular tasks.
How It Works
Fetch Book Data:
Scrapes book details from Amazon and pushes the data to Airflow’s XCom for further processing.

Create PostgreSQL Table:
Checks if the books table exists in the database; if not, it creates it.

Insert Data into PostgreSQL:
Extracts the cleaned data from XCom and inserts it into the books table.

Prerequisites
Install Dependencies:
Ensure you have the following installed:

Python: Version 3.7+
Apache Airflow: Installed and configured.
PostgreSQL: A running PostgreSQL instance.
Python Libraries: Install the required libraries using:
pip install requests beautifulsoup4 pandas psycopg2-binary  
Airflow Connection Setup:
Set up a PostgreSQL connection in the Airflow UI with the Connection ID: books_connection.
Setup and Execution
1. Clone the Repository:
git clone https://github.com/your-username/amazon-books-pipeline.git  
cd amazon-books-pipeline  
2. Upload the DAG to Airflow:
Place the Amazon_Books.py file in your Airflow DAGs directory:

cp Amazon_Books.py /path/to/airflow/dags/  
3. Run the DAG:
Open the Airflow UI.
Trigger the fetch_and_store_amazon_books DAG.
Project Workflow
graph TD  
    A[Fetch Book Data] --> B[Create PostgreSQL Table]  
    B --> C[Insert Data into PostgreSQL]  
Directory Structure
amazon-books-pipeline/  
│  
├── Amazon_Books.py      # Airflow DAG defining the ETL pipeline  
├── README.md            # Project documentation  
└── requirements.txt     # Python dependencies (optional)  
Technologies Used
Technology	Purpose
Python	Web scraping and data processing
Apache Airflow	ETL orchestration
PostgreSQL	Data storage and retrieval
Requests	Fetching web pages
BeautifulSoup	Parsing and extracting data from HTML
Pandas	Data cleaning and transformation
Future Enhancements
Add support for additional data points (e.g., descriptions, reviews).
Integrate the pipeline with cloud storage (e.g., AWS S3).
Implement advanced monitoring with Airflow logs and alerts.
License
This project is licensed under the MIT License.

Contributions
Contributions, issues, and feature requests are welcome! Feel free to fork this repository and submit a pull request.
