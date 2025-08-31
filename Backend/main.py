from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import sys
import httpx
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict

sys.path.append(str(Path(__file__).resolve().parent.parent))
from prediction import predict_hazard
import geocoder

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------
# Pydantic Models
# -------------------
class Location(BaseModel):
    city: str

class WeatherResponse(BaseModel):
    city: str
    hazard_prediction: str
    weather_data: Dict

class SubscriberRequest(BaseModel):
    name: str
    phone_number: str
    place: str

# -------------------
# Database setup
# -------------------
DATABASE_URL = "sqlite:///./subscribers.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

class Subscriber(Base):
    __tablename__ = "subscribers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    phone_number = Column(String, unique=True)
    place = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# -------------------
# API Endpoints
# -------------------
@app.post("/predict")
async def predict(location: Location):
    """Get hazard prediction and current weather data for a city"""
    g = geocoder.opencage(location.city, key="c67aca782f414b84bff7138b49ece2fe")
    if not g.ok:
        raise HTTPException(status_code=404, detail="Could not find coordinates for this city")
    
    lat, lon = g.latlng[0], g.latlng[1]
    
    # Get hazard prediction
    hazard = predict_hazard(lat, lon)
    
    # Get weather data from Open-Meteo API
    weather_data = await get_weather_data(lat, lon)
    
    return {
        "city": location.city,
        "hazard_prediction": hazard,
        "weather_data": weather_data,
        "coordinates": {"lat": lat, "lon": lon}
    }

@app.get("/weather/{city}")
async def get_weather_only(city: str):
    """Get only weather data for a city (for chart updates)"""
    g = geocoder.opencage(city, key="c67aca782f414b84bff7138b49ece2fe")
    if not g.ok:
        raise HTTPException(status_code=404, detail="Could not find coordinates for this city")
    
    lat, lon = g.latlng[0], g.latlng[1]
    weather_data = await get_weather_data(lat, lon)
    
    return {
        "city": city,
        "weather_data": weather_data,
        "coordinates": {"lat": lat, "lon": lon}
    }

@app.post("/subscribers")
def register_subscriber(req: SubscriberRequest):
    """Register a subscriber for alerts"""
    db = SessionLocal()
    subscriber = Subscriber(
        name=req.name,
        phone_number=req.phone_number,
        place=req.place
    )
    db.add(subscriber)
    db.commit()
    return {"message": "Subscriber saved successfully"}

@app.get("/subscribers")
def get_subscribers():
    """Get all subscribers"""
    db = SessionLocal()
    subs = db.query(Subscriber).all()
    return subs

# -------------------
# Helper Functions
# -------------------
async def get_weather_data(lat: float, lon: float) -> Dict:
    """Fetch weather data from Open-Meteo API"""
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    start_date = start_time.strftime('%Y-%m-%d')
    end_date = end_time.strftime('%Y-%m-%d')
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,pressure_msl,cloud_cover",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "auto"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            hourly_data = data.get("hourly", {})
            
            # Last 24 data points
            times = hourly_data.get("time", [])[-24:]
            temperatures = hourly_data.get("temperature_2m", [])[-24:]
            humidity = hourly_data.get("relative_humidity_2m", [])[-24:]
            wind_speed = hourly_data.get("wind_speed_10m", [])[-24:]
            precipitation = hourly_data.get("precipitation", [])[-24:]
            pressure = hourly_data.get("pressure_msl", [])[-24:]
            cloud_cover = hourly_data.get("cloud_cover", [])[-24:]
            
            formatted_times = [
                datetime.fromisoformat(t.replace('T', ' ')).strftime('%H:%M') for t in times
            ]
            
            return {
                "timestamps": formatted_times,
                "temperature": temperatures,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "precipitation": precipitation,
                "pressure": pressure,
                "cloud_cover": cloud_cover,
                "current": {
                    "temperature": temperatures[-1] if temperatures else None,
                    "humidity": humidity[-1] if humidity else None,
                    "wind_speed": wind_speed[-1] if wind_speed else None,
                    "precipitation": precipitation[-1] if precipitation else None
                }
            }
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return {
            "timestamps": [],
            "temperature": [],
            "humidity": [],
            "wind_speed": [],
            "precipitation": [],
            "pressure": [],
            "cloud_cover": [],
            "current": {},
            "error": str(e)
        }

# -------------------
# Run app
# -------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
