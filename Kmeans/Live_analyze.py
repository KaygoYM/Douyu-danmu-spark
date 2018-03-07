# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 15:11:17 2018

@author: KAI
"""

import numpy as np
from sklearn.preprocessing import MinMaxScaler
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.clustering import KMeans
import matplotlib.pyplot as plt
import matplotlib as mlp
from itertools import combinations as comb
#%%=====参数设定======#
mlp.rcParams['font.family']='sans-serif'  
mlp.rcParams['font.sans-serif']=[u'SimHei']
mlp.rcParams['axes.labelsize']=20
mlp.rcParams['xtick.labelsize']=20
mlp.rcParams['ytick.labelsize']=20
#%%=======准备工作=======#
Conf=SparkConf().setMaster("local").setAppName("Live_analyze")
sc=SparkContext(conf=Conf)
ticks=['弹幕量/分钟','关注增长/分钟','收入/分钟','观众数','热度中值']
raw_data=sc.textFile("data.txt")
data=raw_data.map(lambda x:x.strip().split('|'))
date_label=data.map(lambda x:x[-1]).collect()#日期
data_features=data.map(lambda x:x[0:-1])
#计算单位时间的量
data_KNN=np.array(data_features.collect()).astype(np.float)
for x in range(0,3):
    data_KNN[:,x]=data_KNN[:,x]/data_KNN[:,-1]
data_KNN=MinMaxScaler().fit_transform(data_KNN[:,0:-1])#min-max归一化

LiveVectors=sc.parallelize(data_KNN)#RDD形式
Plotxx=np.array(LiveVectors.collect())#画图用
#%%=======训练模型========#
model=KMeans.train(LiveVectors,5,initializationMode='k-means||',maxIterations=50,runs=10,epsilon=10e-6)#5个簇，可自定
print("Final centers: ",model.clusterCenters)
print("Total Cost: " + str(model.computeCost(LiveVectors)))
#pca = PCA(n_components=2)  
#xx=pca.fit_transform(data_KNN)
#print(xx)
prediction=model.predict(LiveVectors)#RDD形式
labels=prediction.collect()
print(labels)
K=10#5个量10副画
plt.figure(figsize=(10,10))
comb_list=np.array(list(comb(range(5),2)))#排列组合
#%%=====画图=======#
for i in range(1,K+1):
    ax=plt.subplot(4,3,i)
    x=comb_list[i-1,0]
    y=comb_list[i-1,1]
    ax.scatter(Plotxx[:,x],Plotxx[:,y],c=labels)
    ax.scatter(np.array(model.clusterCenters)[:,x],np.array(model.clusterCenters)[:,y],c='red',marker='*',s=25)
    for a,b,j in zip(Plotxx[:,x],Plotxx[:,y],range(data.count())):
        ax.text(a, b+0.0005,date_label[j], ha='center', va= 'bottom',fontsize=12)
    ax.set_xlabel(ticks[x])
    ax.set_xlim(0,1)
    ax.set_ylabel(ticks[y])
    ax.set_ylim(0,1)
    ax.axis('equal')
    plt.tight_layout()
    
#%%====雷达图======#
values = np.array(model.clusterCenters)+0.0005
feature = ticks
N = len(values.T)
# 设置雷达图的角度，用于平分切开一个圆面
angles=np.linspace(0,2*np.pi,N,endpoint=False)
# 为了使雷达图一圈封闭起来，需要下面的步骤
s=values[:,0]
s=s[:,np.newaxis]
values=np.hstack((values,s))
angles=np.concatenate((angles,[angles[0]]))
# 绘雷达图
fig=plt.figure(figsize=(8,8))
ax = fig.add_subplot(111, polar=True)# 这里一定要设置为极坐标格式
for i in range(len(values)):
    ax.plot(angles, values[i,:],linewidth=2,label='%s'%(i,))# 绘制折线图
    ax.fill(angles, values[i,:], alpha=0.25)# 填充颜色
ax.set_thetagrids(angles * 180/np.pi, feature)# 添加每个特征的标签
ax.set_ylim(0,1)# 设置雷达图的范围
ax.grid(True)# 添加网格线
plt.legend(loc='best')
plt.show()
#%%=======月话痨统计=======#
T=11#天数
room_id=687423#房间号
#弹幕文件以RDD形式整合
danmu=sc.textFile(str(room_id)+' (1).txt')
for i in range(2,T+1):
    danmu_new=sc.textFile(str(room_id)+' (%d).txt'%i)
    danmu=danmu.union(danmu_new)

danmu_content=danmu.map(lambda x:x.strip().split('->'))
#print(danmu.collect())
NUM=10#榜单TOP NUM
#id+nickname作为一个整体出现
Tops=danmu_content.map(lambda x:((int(x[0]),x[1]),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
#Tops=Tops.filter(lambda x:x[1]>=1000)#大于等于1000条的话痨
print('话痨榜: ',Tops.take(NUM))
temp=Tops.first()#榜单第一
Top_content=danmu_content.filter(lambda x:x[1]==temp[0][1]).map(lambda x:x[3]).collect()#榜单第一的所有弹幕
content=",".join(Top_content)
from wordcloud import WordCloud
wc = WordCloud(background_color='white',width=1200,height=860,font_path = './fonts/simhei.ttf').generate(content)
plt.imshow(wc)
plt.axis('off')
plt.show()