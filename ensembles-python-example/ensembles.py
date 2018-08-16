# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 09:38:15 2018

@author: toshiba
"""

# Kütüphaneleri İndirme, Çalışma Dizinini Ayarlama, Veri Setini İndirme
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
os.chdir('C:\\maven-projects\\veribilimiokulu\\blog\\ensembles-python-example')

data = pd.read_csv('SosyalMedyaReklamKampanyası.csv')

# 4.2. Veri Setini Bağımlı ve Bağımsız Niteliklere Ayırmak
X = data.iloc[:, [2,3]].values
y = data.iloc[:, 4].values

# 4.3. Veri Setini Test ve Eğitim Olarak Ayırma
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.20, random_state = 0)

# 4.4. Karar Ağacı Modelini Eğitme
from sklearn.tree import DecisionTreeClassifier
decisionTreeObject = DecisionTreeClassifier()
decisionTreeObject.fit(X_train,y_train)

# 4.5. Karar Ağacı Modelini Test Etme
dt_test_sonuc = decisionTreeObject.score(X_test, y_test)
print("Karar Ağacı Doğruluk (test_seti): ",round(dt_test_sonuc,2))


# 4.6. Random Forest Modeli Oluşturma
from sklearn.ensemble import RandomForestClassifier
randomForestObject = RandomForestClassifier(n_estimators=10)
randomForestObject.fit(X_train, y_train)
df_test_sonuc = randomForestObject.score(X_test, y_test)
print("Random Forest Doğruluk (test_seti): ",round(df_test_sonuc,2))


# 4.7. Ensemble Yöntemler Uygulama: Bagging
from sklearn.ensemble import BaggingClassifier

baggingObject = BaggingClassifier(DecisionTreeClassifier(), max_samples=0.5, max_features=1.0, n_estimators=20)
baggingObject.fit(X_train, y_train)
baggingObject_sonuc = baggingObject.score(X_test, y_test)
print("Bagging Doğruluk (test_seti): ", round(baggingObject_sonuc,2))