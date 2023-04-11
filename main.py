import json
import mysql.connector
import requests
import langdetect
from langid import langid
from concurrent.futures import ThreadPoolExecutor


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


# Define a function to request data from logs.tf API
def get_log_data(log_id):
    url = "https://logs.tf/json/" + str(log_id)
    response = requests.get(url)
    # Check if the response is valid
    if response.status_code != 200:
        print(f"Log {log_id} not found. Moving to the next log.")
        return None
    data = json.loads(response.content)
    return data


# Load JSON data from logs.tf API
start = 3371607
end = 3188151

with ThreadPoolExecutor(max_workers=10) as executor:
    # Submit requests to executor
    futures = [executor.submit(get_log_data, i) for i in range(start - 2500, end, -1)]

    # Iterate through futures and call writetodb function for each completed future
    for future in futures:
        data = future.result()
        if data is not None:
            writetodb(data)



