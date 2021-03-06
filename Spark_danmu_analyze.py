﻿# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 21:18:23 2018

@author: KAI
"""
#%%============准备工作=========#
from pyspark import SparkConf
from pyspark import SparkContext
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import pandas as pd
import matplotlib as mlp
import numpy as np
import time
  
mlp.rcParams['font.family']='sans-serif'  
mlp.rcParams['font.sans-serif']=[u'SimHei']
mlp.rcParams['axes.labelsize']=20
mlp.rcParams['xtick.labelsize']=20
mlp.rcParams['ytick.labelsize']=20
mlp.rcParams['figure.figsize']=(8,6)

rid=input('输入房间号：')
txt_name=str(rid)+'_'+str(time.strftime("%d_%m_%Y"))+'.txt'
#txt_name=str(rid)+'_04_02_2018.txt'
#txt = open(txt_name+'.txt','r',encoding='gbk')

conf = SparkConf().setMaster("local[*]").setAppName("DouyuApp")
sc = SparkContext(conf=conf)#local本地URL #APPname

#为了解决sc读gbk的问题
danmulist=sc.textFile(txt_name)
danmu_data=danmulist.map(lambda lines:lines.strip('\n').split('->'))
danmu_data.cache()
#建立Result.txt文件，准备写入结果
result=open('Result_'+txt_name+'.txt','a+')

#%%=========总弹幕数===========#
Total_danmu_num=danmu_data.count()
print("弹幕总数:",Total_danmu_num,file=result)

#%%=========总观众数===========#
Total_pop=danmu_data.map(lambda x:x[0]).distinct().count()
print("观众老爷总数:",Total_pop,file=result)#以id统计

#%%==========弹幕颜色============#
'''
colors=danmu_data.map(lambda x:(int(x[-2]),1)).reduceByKey(lambda x,y:x+y).collect()
colors=np.array(colors)
#print(colors)
white_num=colors[0,1]
other_num=sum(colors[1:,1])
print('白色弹幕：',white_num,file=result)
print('有色弹幕: ',other_num,file=result)
for i in colors[1:]:
    print("%s,有 %d 条,占有色弹幕的 %.1f %%"%(translate_to_color.ttc(i[0]),i[1],i[1]/other_num*100),file=result)
'''
#%%============话痨===============#
NUM=5#榜单TOP NUM

#id+nickname作为一个整体出现
Tops=danmu_data.map(lambda x:((int(x[0]),x[1]),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False).take(NUM)
print('话痨榜: ',Tops,file=result)

#%%===========观众等级分布============#
#id+level作为筛选标准
levels=danmu_data.map(lambda x:(int(x[0]),int(x[2]))).groupByKey().map(lambda x:(x[0],np.ceil(np.median(list(x[1])))))
levels=np.array(levels.collect())[:,1]
print("观众等级中值：%d"%(np.median(levels),),file=result)
lbins=np.arange(min(levels),max(levels),2)
plt.figure(1)
plt.hist(levels,lbins,histtype='bar',facecolor='orange',edgecolor='black',alpha=0.75,rwidth=0.8)
plt.xlabel("等级区间")
plt.xlim(min(levels),max(levels))
plt.ylabel("出现频率") 
#plt.title("观众等级分布")
plt.savefig('%s Audience level.png'%(txt_name,),dpi=300)

#%%==========观众前五牌子比例======#
Badges=danmu_data.filter(lambda x:x[-2]!='NONE')
#id+badge作为一个整体，不重复计数
Top_badges_list=Badges.map(lambda x:(int(x[0]),x[-2])).distinct().groupBy(lambda x:x[1]).map(lambda x:(x[0],len(x[1]))).sortBy(lambda x:x[1],ascending=False).take(NUM)
print(Top_badges_list[0][0]+"的牌子数共",Top_badges_list[0][1],file=result)
#以下为画图
Top_badges=np.array(Top_badges_list)
labels=Top_badges[:,0]
sizes=Top_badges[:,1].astype(np.int)/sum(Top_badges[:,1].astype(np.int))
colors='lightcoral','gold','lightskyblue','yellow','yellowgreen'
explode=0.5,0.4,0.2,0.2,0
plt.figure(2)
_,t_text,p_text=plt.pie(tuple(sizes),labels=tuple(labels), explode=explode,colors=colors, autopct='%1.1f%%',
        shadow=True, startangle=50)
for p,t in zip(p_text,t_text):
    t.set_size=(40)
    p.set_size=(40)
plt.axis('equal')
#plt.title("观众前五牌子比例")
plt.savefig('%s TOP5 badge.png'%(txt_name,),dpi=300)

#%%=========观众牌子等级分布==========#
#id+badge_level作为一个整体
badge_levels_raw=Badges.filter(lambda x:x[-2]=='%s'%(Top_badges_list[0][0],))
badge_levels=badge_levels_raw.map(lambda x:(x[0],int(x[-1]))).groupByKey().map(lambda x:(x[0],np.ceil(np.median(list(x[1])))))
blevels=np.array(badge_levels.collect())[:,1]
blevels=blevels.astype(np.float)
print("牌子的平均等级：%1.1f"%(np.mean(blevels),),file=result)
bbins=np.arange(min(blevels),max(blevels),1)
plt.figure(3)
plt.hist(blevels,bbins,histtype='bar',facecolor='darkblue',edgecolor='black',alpha=0.75,rwidth=0.8)
plt.xlabel("等级区间")
plt.ylabel("出现频率") 
#plt.title("%s牌子等级分布"%(labels[0],))
plt.savefig('%s badge level.png'%(txt_name,),dpi=300)

#%%=========弹幕热词============#
import jieba.analyse as ana
#jieba 中文分词
import jieba.posseg as psg
danmu_content=danmu_data.map(lambda x:x[3]).collect()
#print(danmu_content)
content=" ".join(danmu_content)
#print(content)
#danmu_words=jieba.cut_for_search(content)
danmu_words_flags=[(x.word,x.flag) for x in psg.cut(content)]#获取词的属性以便过滤
stop_attr = ['a','b','c','d','f','df','p','r','rr','s','t','u','ule','ude1','v','z','x','y','e']
stop_word = ['了','的','吧','吗','个','人','部','1','2','3','4','一']
# 过滤掉不需要的词性的词
Topwords = [x[0] for x in danmu_words_flags if x[1] not in stop_attr and x[0] not in stop_word]
from collections import Counter
c = Counter(Topwords).most_common(50)
#以词云显示
from wordcloud import WordCloud,STOPWORDS,ImageColorGenerator
'''
text=dict(c)
backgroud_Image = plt.imread('cover_chicken.jpg')
#backgroud_Image = plt.imread('cover_love.jpg')
wc = WordCloud( background_color = 'white',    # 设置背景颜色
                mask = backgroud_Image,        # 设置背景图片
                max_words = 200,            # 设置最大现实的字数
                stopwords = STOPWORDS,        # 设置停用词
                font_path = './fonts/simhei.ttf',# 设置字体格式，如不设置显示不了中文
                max_font_size = 30,# 设置字体最大值
                width=1000,
                height=860,
                #min_font_size = 10,               
                random_state = 24,            # 设置有多少种随机生成状态，即有多少种配色方案
                )
wc.generate_from_frequencies(text)
image_colors = ImageColorGenerator(backgroud_Image)
wc.recolor(color_func = image_colors)
'''
text=dict(c)
wc = WordCloud(background_color="lightyellow",width=1200, height=860, margin=2,font_path = './fonts/simhei.ttf',).generate_from_frequencies(text)
#wc = WordCloud(background_color="lightyellow",width=1200, height=860, margin=2,font_path = './fonts/simhei.ttf',
 #              stopwords=STOPWORDS).generate(content)
plt.imshow(wc)
plt.axis('off')
plt.savefig('%s hot_words.png'%(txt_name,),dpi=600)
print('今日热词: ',c,file=result)
#关键词提取
#jieba.analyse.extract_tags(sentence,topK,withWeight) #需要先 import jieba.analyse TF-IDF算法
#Textrank算法类似于Pagerank
Keywords=ana.textrank(content, topK = 20, withWeight = False, allowPOS = ('ns', 'n', 'v', 'nv')) #注意默认过滤词性。
print('今日弹幕关键词: ',Keywords,file=result)

#%%============贵族弹幕===========#
'''
vip=danmu_data.filter(lambda x:x[-1]=='b\'1\'')
#Tops=danmu_data.map(lambda x:((int(x[0]),x[1]),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False).take(NUM)
vip_content=vip.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False).take(NUM)
#print(vip.collect())
print("贵族弹幕有 ",vip.count(),file=result)
print("VIP节奏位 ",vip_content,file=result)
'''
#%%=========关注数变化========热度变化===========#
txt_name_room=str(rid)+'_'+str(time.strftime("%d_%m_%Y"))+'room.txt'
#txt_name_room=str(rid)+'_04_02_2018room.txt'
roominformation=sc.textFile(txt_name_room)
roominfo=roominformation.map(lambda lines:lines.strip('\n').split('|'))
roominfo=np.array(roominfo.collect())[:,0:2].astype(np.int)
#print(roominfo)
hot=roominfo[:,0]
fans=roominfo[:,1]

print('今日关注量增长约: ',fans[-1]-fans[0],file=result)
print('今日热度峰值: ',np.max(hot),file=result)
print('今日热度中值: ',round(np.median(hot)),file=result)
#plt.figure(4)
fig,ax1 = plt.subplots()
ax2 = ax1.twinx()
#ax1.xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m-%d %H:%M:%S'))#设置时间标签显示格式
#plt.xticks(pd.date_range('2018-02-04 09:00:44','2018-02-04 15:54:44',freq='5min'))#时间间隔
plt.xticks(rotation=30)
ax1.plot(fans, 'ro-',label='关注量',linewidth=3.0)
ax2.plot(hot, 'm^-',label='热度',linewidth=3.0)
ax1.set_xlabel('时间序列')
ax1.set_ylabel('关注量')
ax2.set_ylabel('热度')
#ax1.legend(loc=2,prop={'size': 6}) 
#ax2.legend(loc=1,prop={'size': 6})
#plt.title("主播热度和关注量的变化")
plt.savefig('%s hot and fans.png'%(txt_name,),dpi=300)

#%%========礼物=============#
txt_name_gift=str(rid)+'_'+str(time.strftime("%d_%m_%Y"))+'gift.txt'
#txt_name_gift=str(rid)+'_04_02_2018gift.txt'
giftlist=sc.textFile(txt_name_gift)
gift_info=giftlist.map(lambda lines:lines.strip('\n').split('->'))
gift=gift_info.groupBy(lambda x:x[1]).map(lambda x:(x[0],len(x[1])))
print("礼物统计: ",gift.collect(),file=result)
gift_vip=gift_info.map(lambda x:((x[0],x[1]),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False).take(NUM)
print("礼物榜: ",gift_vip,file=result)
#%%==========收尾===========#
result.close()
plt.show()