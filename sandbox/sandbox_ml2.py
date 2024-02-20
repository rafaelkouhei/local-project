# Machine Learning Practice
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.dummy import DummyClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import export_graphviz

# Load and prepare data
uri = "https://gist.githubusercontent.com/guilhermesilveira/4d1d4a16ccbf6ea4e0a64a38a24ec884/raw/afd05cb0c796d18f3f5a6537053ded308ba94bf7/car-prices.csv"
data = pd.read_csv(uri)
swap_data = {'yes': 1, 'no': 0}
data['sold'] = data['sold'].map(swap_data)
current_year = datetime.today().year
data['age'] = current_year - data['model_year']
data['km_per_year'] = data['mileage_per_year']*1.60934
data = data.drop(columns=['Unnamed: 0', 'mileage_per_year', 'model_year'], axis=1)
# print(data)
x = data[['km_per_year', 'age', 'price']]
y = data['sold']
np.random.seed(42)
raw_train_x, raw_test_x, train_y, test_y = train_test_split(x, y, test_size=0.25, stratify=y)

# Scaler
scaler = StandardScaler()
scaler.fit(raw_train_x)
train_x = scaler.transform(raw_train_x)
test_x = scaler.transform(raw_test_x)

# Linear Model
linear_model = LinearSVC()
linear_model.fit(raw_train_x, train_y)
linear_prediction = linear_model.predict(raw_test_x)
print(f'Linear Model Accuracy Ratio: {round(linear_model.score(raw_test_x, test_y)*100, 2)}%')

# Baseline
baseline = np.ones(len(raw_test_x))
print(f'Baseline Accuracy Ratio: {round(accuracy_score(test_y, baseline)*100, 2)}%')

# Dummy Model
dummy = DummyClassifier(strategy='most_frequent')
dummy.fit(raw_train_x, train_y)
dummy_prediction = dummy.predict(raw_test_x)
print(f'Dummy Accuracy Ratio: {round(dummy.score(raw_test_x, test_y)*100, 2)}%')

# SVC Model
svc_model = SVC()
svc_model.fit(train_x, train_y)
svc_prediction = svc_model.predict(test_x)
print(f'SVC Model Accuracy Ratio: {round(svc_model.score(test_x, test_y)*100, 2)}%')

# Decision Tree Classifier Model
tree_model = DecisionTreeClassifier(max_depth=3)
tree_model.fit(train_x, train_y)
print(f'Decision Tree Model Accuracy Ratio: {round(tree_model.score(test_x, test_y)*100, 2)}%')
graph = export_graphviz(tree_model, out_file=None)