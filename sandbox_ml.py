### Machine Learning Practice
import pandas as pd
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score

### 1. Features - is it important to list the key characteristics of the thing that you want to analyse
# For example, you want to know if someone is a child (-18) or adult human, so:
# 1 - Do they have enough height? (1,6m)?; 2 - Do they have wrinkles?; 3 - Do they have a defined jawline?; 4 - Do they have a degree?; 5 - Do they have a job?; 6 - Do they drive?
dan = [1, 0, 1, 1, 0, 1] # Daniel 17
ash = [0, 0, 0, 0, 0, 0] # Ashley 14
kim = [0, 1, 0, 0, 1, 1] # Kimberly 20
alb = [1, 1, 1, 1, 0, 1] # Albert 55
hug = [0, 0, 1, 1, 1, 0] # Hugo 21
els = [1, 0, 1, 1, 0, 1] # Elsa 25
mik = [1, 1, 1, 0, 1, 0] # Mikhaela 28
sim = [1, 0, 1, 0, 0, 1] # Simba 24
nal = [1, 0, 1, 0, 1, 1] # Nala 23
woo = [1, 0, 1, 0, 0, 0] # Woody 11
stc = [0, 0, 0, 0, 0, 1] # Stitch 5
ral = [1, 0, 1, 0, 0, 0] # Ralph 13
wit = [0, 1, 1, 0, 1, 0] # Witch 128

# 0 for child and 1 for adult, because in a supervised machine learning training, we have to tell the actual classes to the machine
train_x = [dan, ash, kim, alb, hug, els, mik, sim, nal, woo, stc, ral]
train_y = [0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0]
model = LinearSVC()
model.fit(train_x, train_y)

hrc = [1, 0, 1, 0, 1, 1] # Hercules 18
scr = [1, 1, 1, 1, 0, 0] # Scar 41
lil = [0, 0, 0, 0, 0, 0] # Lilo 7
mul = [0, 0, 1, 0, 1, 1] # Mulan 20
test_x = [hrc, scr, lil, mul]
predict1 = model.predict(test_x)
# print(predict1)
# actual1 = [1, 1, 0, 0, 0]
# print(predict1 == actual1)
# acc_ratio = accuracy_score(actual1, predict1)
# print(acc_ratio)

###
uri = "https://gist.githubusercontent.com/guilhermesilveira/2d2efa37d66b6c84a722ea627a897ced/raw/10968b997d885cbded1c92938c7a9912ba41c615/tracking.csv"
data = pd.read_csv(uri)
data_x = data[['home', 'how_it_works', 'contact']]
data_y = data['bought']
print(data_x)
print(data_y)