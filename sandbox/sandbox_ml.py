# Machine Learning Practice
import pandas as pd
import numpy as np
from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# It's important to list the key characteristics (features) of the thing that you want to analyse
# For example, you want to know if someone is a child (-18) or adult human, so:
# 1 - Do they have enough height? (1,6m)?; 2 - Do they have wrinkles?; 3 - Do they have a defined jawline?; 4 - Do they have a degree?; 5 - Do they have a job?; 6 - Do they drive?
dan = [1, 0, 1, 1, 0, 1]  # Daniel 17
ash = [0, 0, 0, 0, 0, 0]  # Ashley 14
kim = [0, 1, 0, 0, 1, 1]  # Kimberly 20
alb = [1, 1, 1, 1, 0, 1]  # Albert 55
hug = [0, 0, 1, 1, 1, 0]  # Hugo 21
els = [1, 0, 1, 1, 0, 1]  # Elsa 25
mik = [1, 1, 1, 0, 1, 0]  # Mikaela 28
sim = [1, 0, 1, 0, 0, 1]  # Simba 24
nal = [1, 0, 1, 0, 1, 1]  # Nala 23
woo = [1, 0, 1, 0, 0, 0]  # Woody 11
stc = [0, 0, 0, 0, 0, 1]  # Stitch 5
ral = [1, 0, 1, 0, 0, 0]  # Ralph 13
wit = [0, 1, 1, 0, 1, 0]  # Witch 128
bel = [1, 0, 0, 1, 1, 0]  # Bella 20
bst = [1, 1, 1, 1, 0, 0]  # Beast 45
mgl = [0, 0, 0, 0, 0, 0]  # Miguel 12
hec = [1, 1, 1, 0, 1, 0]  # Hector 250
bmx = [1, 0, 0, 0, 1, 1]  # Beymax 1
tks = [1, 0, 1, 1, 1, 1]  # Takashi 25
hro = [0, 0, 0, 0, 1, 0]  # Hiro 10
hrc = [1, 0, 1, 0, 1, 1]  # Hercules 18
scr = [1, 1, 1, 1, 0, 0]  # Scar 41
lil = [0, 0, 0, 0, 0, 0]  # Lilo 7
mul = [0, 0, 1, 0, 1, 1]  # Mulan 20
shg = [1, 1, 1, 1, 1, 1]  # Shang 29

# 0 for child and 1 for adult, because in a supervised machine learning training, we have to tell the actual classes to the machine
x = [dan, ash, kim, alb, hug, els, mik, sim, nal, woo, stc, ral, wit, bel, bst, mgl, hec, bmx, tks, hro, hrc, scr, lil, mul, shg]
y = [0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 1]
train_ppl_x, test_ppl_x, train_ppl_y, test_ppl_y = train_test_split(x, y, test_size=0.25, random_state=42, stratify=y) # splitting training and testing units
model_ppl = LinearSVC()
model_ppl.fit(train_ppl_x, train_ppl_y)
predict_ppl = model_ppl.predict(test_ppl_x)
predict1 = model_ppl.predict(x)
acc_ratio_ppl = accuracy_score(y, predict1)
print(predict1 == y)
print(f'Accuracy Ratio: {round(acc_ratio_ppl * 100, 2)}%')
print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Practicing with external datas
uri='https://gist.githubusercontent.com/guilhermesilveira/2d2efa37d66b6c84a722ea627a897ced/raw/10968b997d885cbded1c92938c7a9912ba41c615/tracking.csv'
data = pd.read_csv(uri)
data_x = data[['home', 'how_it_works', 'contact']]
data_y = data['bought']

# In case you want to rename the columns name
column_rename = {'home': 'ホーム', 'how_it_works': '紹介', 'contact': '連絡先', 'bought': '買った'}
data = data.rename(columns = column_rename)

# There's 99 lines in this data, but we don't want to use all the data to train it. So, we are going to use 75% of the data to train, and then the rest of 25% to test it.
train_ext_x, test_ext_x, train_ext_y, test_ext_y = train_test_split(data_x, data_y, test_size=0.25, random_state=42, stratify=data_y)
model_ext = LinearSVC()
model_ext.fit(train_ext_x, train_ext_y)

predict_ext = model_ext.predict(data_x)
acc_ratio_ext = accuracy_score(data_y, predict_ext)
print(np.array(predict_ext == data_y))
print(f'Accuracy Ratio: {round(acc_ratio_ext * 100, 2)}%')
print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
uri2 = "https://gist.githubusercontent.com/guilhermesilveira/1b7d5475863c15f484ac495bd70975cf/raw/16aff7a0aee67e7c100a2a48b676a2d2d142f646/projects.csv"
ext2 = pd.read_csv(uri2)
swap_ext2 = {0: 1, 1: 0}
ext2['finished'] = ext2['unfinished'].map(swap_ext2)
ext2_x = ext2[['expected_hours', 'price']]
ext2_y = ext2['finished']
SEED = 42
np.random.seed(SEED)
raw_train_ext2_x, raw_test_ext2_x, train_ext2_y, test_ext2_y = train_test_split(ext2_x, ext2_y, test_size=0.25, stratify=ext2_y)
model_ext2 = LinearSVC()
model_ext2.fit(raw_train_ext2_x, train_ext2_y)
predict_ext2 = model_ext2.predict(raw_test_ext2_x)
acc_ratio_ext2 = accuracy_score(test_ext2_y, predict_ext2)
print(f'LinearSVC Accuracy Ratio: {round(acc_ratio_ext2*100, 2)}%')
# baseline accuracy
hrs_min = raw_test_ext2_x['expected_hours'].min()
hrs_max = raw_test_ext2_x['expected_hours'].max()
price_min = raw_test_ext2_x['price'].min()
price_max = raw_test_ext2_x['price'].max()
x_axis = np.arange(hrs_min, hrs_max, (hrs_max-hrs_min)/100)
y_axis = np.arange(price_min, price_max, (price_max-price_min)/100)
xx, yy = np.meshgrid(x_axis, y_axis)
dots = np.c_[xx.ravel(), yy.ravel()]
z = model_ext2.predict(dots)
z = z.reshape(xx.shape)

scaler = StandardScaler()
scaler.fit(raw_train_ext2_x)
train_ext2_x = scaler.transform(raw_train_ext2_x)
test_ext2_x = scaler.transform(raw_test_ext2_x)

new_model = SVC(gamma='auto')
new_model.fit(train_ext2_x, train_ext2_y)
new_predict = new_model.predict(test_ext2_x)
acc_ratio_new = accuracy_score(test_ext2_y, new_predict)
print(f'SVC Accuracy Ratio: {round(acc_ratio_new*100, 2)}%')