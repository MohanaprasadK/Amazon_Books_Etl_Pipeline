import requests
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup

def main():

  headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
  }
  
  #Declaring constant variables
  books = []
  seen_titles = set()  # To keep track of seen titles
  page = 1

  url = f"https://www.amazon.com/s?k=data+engineering+books"

  response = requests.get(url, headers=headers)
  #print(response)

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

      #print("Compiler here at position:1")
      if title and author and price and rating:
        #print("Compiler here position:2")
        book_title = title.text.strip()

        if book_title not in seen_titles:
          #print("Compiler here position:3")
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
  #print(df.head(5))

  df.drop_duplicates(subset="Title", inplace=True)
  #print("Compiler here position:4")

  print("Number of books scrapped : ",df["Title"].count())

#  -------------------------- Entering the main function ----------------------------------------
if __name__ == "__main__":
  print("Welcome to Amazon Books Web Scrapping")
  main()