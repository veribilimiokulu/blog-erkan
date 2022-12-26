from fastapi import FastAPI, Request
from pydantic import BaseModel
import joblib

# Read models saved during train phase
estimator_advertising_loaded = joblib.load("saved_models/03.randomforest_with_advertising.pkl")

class Advertising(BaseModel):
    TV: float
    Radio: float
    Newspaper: float

    class Config:
        schema_extra = {
            "example": {
                "TV": 230.1,
                "Radio": 37.8,
                "Newspaper": 69.2,
            }
        }

app = FastAPI()


def make_advertising_prediction(model, request):
    # parse input from request
    TV = request["TV"]
    Radio = request['Radio']
    Newspaper = request['Newspaper']

    # Make an input vector
    advertising = [[TV, Radio, Newspaper]]

    # Predict
    prediction = model.predict(advertising)

    return prediction[0]


# Advertising Prediction endpoint
@app.post("/prediction/advertising")
def predict_iris(request: Advertising):
    prediction = make_advertising_prediction(estimator_advertising_loaded, request.dict())
    return prediction

