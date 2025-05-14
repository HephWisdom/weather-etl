import requests
import pandas as pd
from sqlalchemy import create_engine

def extract_weather_data(api_key: str, city: str = "London") -> dict:
    
    BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
    
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric" # Get temp in Celsius
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status() #Raise error is API calls fails
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data {e}")
        return{}    
    
    
def transform_weather_data(raw_data: dict) -> pd.DataFrame:
    if not raw_data:
        return pd.DataFrame #this returns an empty frame should extraction fails
    
    #Extract relevant fields
    processed_data = {
        "city": raw_data.get("name", "N/A"),
        "temperature_in_cel": raw_data["main"]["temp"],
        "humidity": raw_data["main"]["humidity"],
        "weather_condition": raw_data["weather"][0]["main"],
        "wind_speed": raw_data["wind"]["speed"],
        "timestamp": pd.to_datetime("now",utc=True) #Record when data was fectched
    }
    print(processed_data)
    
    return pd.DataFrame([processed_data])
    
  

def load_weather_data(df: pd.DataFrame, db_name: str = "weather_db.sqlite") -> None:
    if df.empty:
        print("No data to load")
        return
    try:
        engine = create_engine(f"sqlite:///{db_name}")
        df.to_sql(
            "weather_logs",
            engine,
            if_exists="append",
            index=False
        )
        print(f"Data saved to {db_name}")
    except Exception as e:
        print(f"Database error: {e}")
    
    
    
def main():
    

    API_KEY = "d120e106a9edf5aa040ebc3bd26a1e7c"
    list = ["San Diego","Dhaka","Phoenix","baghdad","Yangon","Kabul"]
    
    for city in list:
        #Extract
        raw_data = extract_weather_data(API_KEY,city=[city])
    
        #Transform
        df = transform_weather_data(raw_data)
    
        #Load
        load_weather_data(df)
        

    
if __name__ == "__main__":
    main()