import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

#################################
# Part 1: Python Data Migration #
#################################

# Read data sheet
data = pd.read_excel('.\\Northwell Mktg Data Assessment\\cia_python_cipher_take_home.xlsx',
                     sheet_name='Data')

# Read cipher sheet
cipher = pd.read_excel('.\\Northwell Mktg Data Assessment\\cia_python_cipher_take_home.xlsx',
                     sheet_name='Cipher')

# Create an iterator
search_array = list(cipher.letter.astype(str))
replace_array = list(cipher.number.astype(str))

# Decode function
def decode(s):
    for search, replace in zip(search_array, replace_array): 
        s = s.replace(search, replace)  # replace each letter when match
    return s

# Iterate through each cell and decode
for i in range(data.shape[0]): # iterate over rows
    for j in range(data.shape[1]): # iterate over columns
        data.iloc[i, j] = decode2(data.iloc[i, j]) # apply decode function to decoded value for each cell

# Another approach to decode
# data['column_1']=[''.join(i.astype(str)) for i in data['column_2'].\
# str.split('',n = -1, expand = True).replace(d).replace([None], "").values]

# Initialize the connection to BQ with the given credentials 
credentials = service_account.Credentials.from_service_account_file(
'.\\Northwell Mktg Data Assessment\\northwell-marcomm-public-data-c9be9aced43e.json')

# Define project id and dataset id
project_id = 'northwell-marcomm-public-data'
dataset_id = 'SQL_Assessment'

# Connect client to the database
client = bigquery.Client(credentials=credentials,project=project_id)

# Define table name for storing the decoded data
table = 'SQL_Assessment.siong_decode'

# Ingest the decoded data into a table
load_data = client.load_table_from_dataframe(data, table)

# Check if siong_decode table created
tables = client.list_tables(dataset_id)

for table in tables:
    if table.table_id=='siong_decode':
        print('Decode table created successfully')


"""

Answer for question #4:

The decoded dataset is the daily total of impressions (column_2), clicks (column_3), and appointment booked (column_4).

Below are the questions I have for the decoded dataset.

1.) Why there are many missing date for the 2020 February data?
2.) Is this data seasonal? The number is higher during cold season.
3.) One thing I notice from the data that appointment booked is always less than clicks, and clicks is always less than impressions.
    Is this always true?

Other data that might be considered useful for analysis or modeling are such as Position, Session, and Pages.

- Position is an attempt to show approximately where on the page a given link was seen, relative to other results on the page.
- Session is the period of time a user is active on a site.
- Pages is the number of pages visitors view on a site within a session.

"""


#################
# Part 2: Query #
#################

# SQL query to recreate exactly the Results table
sql_results = """
   /*Aggregate impressions table*/
   WITH imp_agg AS (
   SELECT EXTRACT(YEAR from PARSE_DATE('%Y%m%d', CAST(date AS STRING))) as year, 
   EXTRACT(MONTH from PARSE_DATE('%Y%m%d', CAST(date AS STRING))) as month, 
   campaign, sum(impressions) as impressions, sum(clicks) as clicks
   FROM SQL_Assessment.MAT_SQL_Assessment_2021_Imp
   GROUP BY year, month, campaign
   ORDER BY year
   ),

   /*Aggregate appointment table*/
   appt_agg AS (
   SELECT EXTRACT(YEAR from booking_datetime) as year, 
   EXTRACT(MONTH from booking_datetime) as month,
   campaign, sum(appt_booked) as appt_booked
   FROM SQL_Assessment.MAT_SQL_Assessment_2021_Appt
   GROUP BY year, month, campaign
   ORDER BY year
   ),

   /*Inner join both aggregated temporary appointment and impressions tables*/
   imp_appt AS (
   SELECT 
   CASE WHEN CAST(imp_agg.year as STRING) != '' THEN CAST(imp_agg.year as STRING) ELSE CAST(appt_agg.year as STRING) END AS year, 
   CASE WHEN CAST(imp_agg.month as STRING) != '' THEN CAST(imp_agg.month as STRING) ELSE CAST(appt_agg.month as STRING) END AS month,
   CASE WHEN imp_agg.campaign != '' THEN imp_agg.campaign ELSE CAST(appt_agg.campaign as STRING) END AS campaign, 
   impressions, clicks, 
   appt_booked,
   FROM imp_agg
   RIGHT JOIN appt_agg
   ON appt_agg.year = imp_agg.year AND
   appt_agg.month = imp_agg.month AND
   appt_agg.campaign = imp_agg.campaign
   ORDER BY imp_agg.year, imp_agg.month, imp_agg.campaign
   )

   /*Calculate CTR, campaign book rate and campaign book rate to date*/
   SELECT d.year, d.month, d.campaign, d.impressions, d.clicks, 
   d.clicks/d.impressions as click_through_rate,
   d.appt_booked, 
   d.appt_booked / d.clicks as month_cmpn_book_rate,
   SUM(d.appt_booked) over (partition by d.campaign 
   order by d.year, d.month, d.campaign rows unbounded preceding) / 
   SUM(d.clicks2) over (partition by d.campaign 
   order by d.year, d.month, d.campaign rows unbounded preceding) as cmpn_book_rate_to_date
   FROM (SELECT d.year, d.month, d.campaign, d.impressions, d.clicks, 
   COALESCE(d.clicks,0) as clicks2, d.appt_booked FROM imp_appt as d) AS d
   
   UNION ALL
   
   /*Union all with an existing row and change year to 2023*/
   SELECT CAST(2023 as STRING) as year, t.month, t.campaign, t.impressions, t.clicks, t.click_through_rate, 
   t.appt_booked, t.month_cmpn_book_rate, t.cmpn_book_rate_to_date
   FROM (SELECT d.year, d.month, d.campaign, d.impressions, d.clicks, 
   d.clicks/d.impressions as click_through_rate,
   d.appt_booked, 
   d.appt_booked / d.clicks as month_cmpn_book_rate,
   SUM(d.appt_booked) over (partition by d.campaign 
   order by d.year, d.month, d.campaign rows unbounded preceding) / 
   SUM(d.clicks2) over (partition by d.campaign 
   order by d.year, d.month, d.campaign rows unbounded preceding) as cmpn_book_rate_to_date
   FROM (SELECT d.year, d.month, d.campaign, d.impressions, d.clicks, 
   COALESCE(d.clicks,0) as clicks2, d.appt_booked FROM imp_appt as d) AS d) AS t
   WHERE CAST(t.year as int64) = 2021 AND CAST(t.month as int64) = 2 AND t.campaign = 'ortho'
   ORDER BY CAST(month as int64) ASC, campaign
   """

# Method 1: Create a table from query result
table_id = "northwell-marcomm-public-data.SQL_Assessment.siong_results"
job_config = bigquery.QueryJobConfig(destination=table_id)
query_job = client.query(sql_results, job_config=job_config)
query_job.result()  # Wait for the job to complete.

# Method 2: Load query results into a dataframe and create a table  
df_results = pd.read_gbq(sql_results, project_id=project_id, credentials=credentials, dialect='standard')
table = 'SQL_Assessment.siong_results'
client.load_table_from_dataframe(data, table)
