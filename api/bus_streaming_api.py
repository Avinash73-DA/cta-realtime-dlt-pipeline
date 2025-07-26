import pandas as pd
import requests
import json
from time import sleep
from zoneinfo import ZoneInfo
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

Train_key = 'Enter_Your_API_KEY'
landing_path = "/Volumes//cta_project/landing_volumes/trains/"
url = f'http://lapi.transitchicago.com/api/1.0/ttpositions.aspx?key={Train_key}&rt=red,blue,brn,g,org,p,pink,y&outputType=JSON'

try:
  response = requests.get(url)
  response.raise_for_status()
  raw_json_string = response.text
        
  timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d_%H_%M_%S")
  file_name = f"train_positions_{timestamp}.json"
  full_file_path = f"{landing_path}{file_name}"    

  with open(full_file_path, 'w') as file:
    file.write(raw_json_string)

  print(f"Saved raw train data: {file_name}")

except Exception as e:
  print(f"An error occurred: {e}")
