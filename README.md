# Data Analytics Migration Tableau

In this project, I took on the role of a Data Analyst at Skyscanner that is tasked with ingesting, integrating and cleaning a large dataset containing flight data, loading the cleaned data into a PostgreSQL database, and finally integrating that database with Tableau to create real-time visualisations and dashboards that can be used by top management for strategic decision making.

I used Pandas to ingest 10 years of flight data in sepearte dataframe containing millions of records each. The data for each year was then cleaned in Python using various techniques to remove unwanted or incomplete data. After cleaning the data, due to the immense size of each years data, I took a fraction (10%) of each dataframe for me to work with and saved it. The subsets of the data were then integrated with each other and I saved the output in an integrated CSV file. I then used PostgreSQL on AWS RDS to store the cleaned and transformed data in order to enable data analysis.  I then integrated Tableau with the PostgreSQL RDS to visually explore the data and to create visualisations and various real-time charts (bubble, tree, bar and line) using the back-end flights data stored in RDS. Finally, I prepared a presentation to share with top management highlighting the insights and findings observed.

Python libraries used: pandas

Other systems used: PgAdmin, AWS, Tableau, PostgreSQL