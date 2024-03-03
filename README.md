# stock_data_pipeline

This project is an etl pipeline for loading detatailed stock data.

Its purpose is to exctract data from various sources, transform it
and then load resulting set to relational database.

The data itself consists of following:
Stock quoutes (Yahoo finance API),
Currency quotes (Yahoo finance API),
Lending rates (NewYorkFed.org API),
Treasury rates (Treasury.gov Web Site),
Stock financials (MacroTrends.net Web Site)

Below are technologies I used for this project:
Pandas
Selenium
SQLAlchemy
PostgreSQL
Airflow
BeautifulSoup
FastAPI

The project is deployed on vps, but I will demonstrate it only during interview
as one of the data sources has cloudflare and I need to have a control over
how many requests are sent to it. I hope you understand.
