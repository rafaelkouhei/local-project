import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

uri2 = "https://gist.githubusercontent.com/guilhermesilveira/1b7d5475863c15f484ac495bd70975cf/raw/16aff7a0aee67e7c100a2a48b676a2d2d142f646/projects.csv"
data_uri2 = pd.read_csv(uri2)
# There's a column named unfinished, and it's a bit unnatural. So, I'm going to swap to a column 'finished'
swap_finished = {0: 1, 1: 0}
data_uri2['finished'] = data_uri2['unfinished'].map(swap_finished)
print(data_uri2)
print(data_uri2.columns)
r = sns.scatterplot(x='expected_hours', y='price', data=data_uri2)
plt.show(r)