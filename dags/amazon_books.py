import requests
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def fetch_books_data(ti):

  headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
  }
  
  #Declaring constant variables
  books = []
  seen_titles = set() 
  page = 1

  url = f"https://www.amazon.com/s?k=data+engineering+books"

  response = requests.get(url, headers=headers)

  if response.status_code == 200:
    print("Able to reach the site, scrapping data...")

    soup = BeautifulSoup(response.content, "html.parser")
    book_containers = soup.find_all("div", {"class": "s-result-item"})

    print("The webpage has been scrapped for data...")
    for book in book_containers:

      title = book.find("span", {"class": "a-text-normal"})
      author = book.find("a", {"class": "a-size-base"})
      price = book.find("span", {"class": "a-price-whole"})
      rating = book.find("span", {"class": "a-icon-alt"})

      if title and author and price and rating:
        book_title = title.text.strip()

        if book_title not in seen_titles:
          seen_titles.add(book_title)
          books.append({
              "Title": book_title,
              "Author": author.text.strip(),
              "Price": price.text.strip(),
              "Rating": rating.text.strip(),
          })
      page += 1
    print("Books data has been scrapped from the website succesfully !!!")
  else:
    print("Unable to reach the site, unable to scrape the data *")

  #Convert the list of dictionaries into a DataFrame
  df = pd.DataFrame(books)

  df.drop_duplicates(subset="Title", inplace=True)

  print("Number of books scrapped : ",df["Title"].count())

  ti.xcom_push(key='book_data', value=df.to_dict('records'))

def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_books_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


#-------------------  STARTING WITH DAG FUNCTIONS AND OPERATIONS ---------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Amazon_Book_Data',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

start_task = DummyOperator(
   task_id = 'Start_Task',
   dag=dag,
)

fetch_book_data_task = PythonOperator(
    task_id='Fetch_Books_Data',
    python_callable=fetch_books_data,
    op_args=[],
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='Create_Table_PGdb',
    postgres_conn_id='Amazon_Books',
    sql="""
    CREATE TABLE IF NOT EXISTS amazon_books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

end_task = DummyOperator(
   task_id = 'end_task',
   dag=dag,
)

start_task >> fetch_book_data_task >> create_table_task >> insert_book_data_task >> end_task