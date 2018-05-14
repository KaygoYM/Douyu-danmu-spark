Douyu-danmu-spark
====
Version 3.0||Fin version
----
# Introduction</br>
Compared to The first version of [Douyu_danmu](https://github.com/KaygoYM/Data-Mining/tree/master/Data_Mining_App/DouyuTV), in this repository, the analysis of Douyu_TV's danmu is based on SPARK instead of MYSQL(Pymysql).</br>
# Environment:</br>
Python 3.6</br>
Module jieba, wordcloud</br>
Spyder</br>
SPARK (Pyspark)</br>
Windows10 (64bit)</br>
# HOW TO USE
## Scrapy
In Anaconda Prompt/CMD, print`"python Spark_danmu_scrapy.py"`, and then input the room-id to activate the scrapy process.</br>
OR use the .exe app in the link below.
## Analyze
After the live-broadcast show, stop the scrapy process. In Anaconda Prompt/CMD, print`"python Spark_danmu_analyze.py"`, and then input the room-id to activate the analyze process.</br>
## Result
The results include: Hot Words/The histogram of level/The Top5 badges and so on. Just as shown in ![687423_03_07_2018.jpg](https://github.com/KaygoYM/Douyu-danmu-spark/blob/master/Reports/687423_03_07_2018.jpg)</br> and </br>![156277_01_21_2018.jpg](https://github.com/KaygoYM/Douyu-danmu-spark/blob/master/Reports/156277_01_21_2018.jpg)</br>(two examples)</br>
# Tips
The psd files are the templets that I use to make the daily reports.</br>
Like nvliu66 and yjjimpaopao.Like 156277 and 687423 (ง•̀_•́)ง </br>
# APP

BAIDU CLOUD: 链接(Link): http://t.cn/R8MZkGV 密码(PWD): h5ed </br>
# Further work
Monthly or Yearly Report——By applying [KMEANS](https://github.com/KaygoYM/Douyu-danmu-spark/tree/master/Kmeans) to help host improve the LIVE.</br>
![](https://github.com/KaygoYM/Douyu-danmu-spark/blob/master/Kmeans/Month_Report2.jpg "直播分析报告/数据分析")</br>
