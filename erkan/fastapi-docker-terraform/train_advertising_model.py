import pandas as pd

# read data
df = pd.read_csv("https://raw.githubusercontent.com/erkansirin78/datasets/master/Advertising.csv")
print(df.head())

# Feature matrix
X = df.iloc[:, 1:-1].values
print(X.shape)
print(X[:3])

# Output variable
y = df.iloc[:, -1]
print(y.shape)
print(y[:6])

# split test train
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

# train model
from sklearn.ensemble import RandomForestRegressor

estimator = RandomForestRegressor(n_estimators=200)
estimator.fit(X_train, y_train)

# Test model
y_pred = estimator.predict(X_test)
from sklearn.metrics import r2_score

r2 = r2_score(y_true=y_test, y_pred=y_pred)
print("R2: ".format(r2))

# Save Model
import joblib
joblib.dump(estimator, "saved_models/03.randomforest_with_advertising.pkl")

# make predictions
# Read models
estimator_loaded = joblib.load("saved_models/03.randomforest_with_advertising.pkl")

# Prediction set
X_manual_test = [[230.1,37.8,69.2]]
print("X_manual_test", X_manual_test)

prediction = estimator_loaded.predict(X_manual_test)
print("prediction", prediction)