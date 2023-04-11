import json
import mysql.connector
import requests
import langdetect
from langid import langid


# Define writetodb function outside the loop
def writetodb(data):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="toxicity"
    )
    mycursor = mydb.cursor()

    # Extract relevant data from JSON and save to MySQL database
    for msg in data['chat']:
        message = msg['msg']
        name = msg['name']
        steamid = msg['steamid']
        lang, score = langid.classify(message)  # detect language
        if lang == 'en' and score > 0.9:  # filter non-english messages
         if name.lower() != 'console':
          sql = "INSERT INTO chatlog (steamid, name, message) VALUES (%s, %s, %s)"
          val = (steamid, name, message)
          mycursor.execute(sql, val)
          mydb.commit()

    print("Messages saved to MySQL database!")


# Load JSON data from logs.tf API
start = 3371607
end = 3188151
for i in range(start - 2500, end, -1):
    url = "https://logs.tf/json/" + str(i)
    response = requests.get(url)
    # Check if the response is valid
    if response.status_code != 200:
        print(f"Log {i} not found. Moving to the next log.")
        continue
    data = json.loads(response.content)
    # Call writetodb function for each JSON data
    writetodb(data)

