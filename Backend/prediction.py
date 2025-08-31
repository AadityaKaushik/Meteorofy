# prediction.py
from joblib import load
import numpy as np
from open_meteo_api import get_latest_features
import geocoder

clf = load("clf.joblib")

def predict_hazard(lat, lon):
    latest_features = get_latest_features(lat, lon)
    X = np.array([latest_features.values], dtype=float)
    return clf.predict(X)[0]
    
