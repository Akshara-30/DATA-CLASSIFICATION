


  import sys
	import scipy
	import numpy
	import matplotlib
	import pandas
	import sklearn
	import pandas
	from pandas import read_csv
	from pandas.plotting import scatter_matrix
	from matplotlib import pyplot
	from sklearn.model_selection import train_test_split
	from sklearn.model_selection import cross_val_score
	from sklearn.model_selection import StratifiedKfold
	from sklearn.metrics import classification_report
	from sklearn.metrics import confusion_matrix
	from sklearn.metrics import accuracy_score
	from sklearn.linear_model import LogisticRegression
	from sklearn.tree import DecisionTreeClassifier
	from sklearn.neighbours import KNeighboursClassifier
	from sklearn.model_selection import StratifiedKFold
	from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
	from sklearn.naive_bayes import GaussianNB
	from sklearn.svm import SVC
	from sklearn import model_selection
	from sklearn.ensemble import votingClassifier
	from sklearn.ensemble import VotingClassifier
	url="https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
	names=['sepal_length','sepal_width','petal_length','petal_width','class']
	dataset=read_csv(url,names=names)
	print(dataset.shape)
	print(dataset.head(20))
  print (dataset.describe())
	print(dataset.groupby('class').size())
	#univariate plots_boxandwhiskerplots
	dataset.plot(kind='box',subplots=TRUE,layout=(2,2),sharex=False,sharey=False)
	pyplot.show()
	dataset.hist()
	pyplot.show()
	scatter_matrix(dataset)
	pyplot.show()
	array=dataset.values
	x=array[:,0:4]
	y=array[:,4]
	x_train,x_validation,y_train,y_validation=train_test_split(x,y,test_size=0.2,random_state=1)
	//building model
	model=[]
	models.append(('LR',LogisticRegression(solver='liblinear',multi_class='ovr')))
	models.append(('LDA',LinearDiscriminantAnalysis()))
	models.append(('KNN',KNeighboursClassifier()))
	models.append('NB',GaussianNB())
	models.append(('SVM',SVC(gamma='auto')))
	results=[]
	names=[]
	for name,model in models:
	    Kfold=StratifiedKFold(n_splita=10,random_state=1)
	    cv_results=cross_val_score(model,x_train,y_train,cv=Kfold,scoring='accuracy')
	    results.append(cv_results)
	    names.append(name)
	    print('%s:%f:(%f)'%(name,cv_results.mean(),cv_results.std()))
	pyplot.boxplot(results,labels=names)
	pyplot.title('Algorithm comaprison')
	pyplot.show()
	model=SVC(gamma='auto')
	model.fit(x_train,y_train)
	predictions=model.predict(x_validation)
  print(accuracy_score(y_validation,predictions))
  print(cofusion-matrix(y_validation,predictions))
  print(classification_report(y_validation,predictions))

