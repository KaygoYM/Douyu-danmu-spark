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
## Analyze
After the live-broadcast show, stop the scrapy process. In Anaconda Prompt/CMD, print`"python Spark_danmu_analyze.py"`, and then input the room-id to activate the analyze process.</br>
## Result
The results include: Hot Words/The hist of level/The Top5 badges and so on. Just as shown in [687423_01_11_2018.jpg](https://github.com/KaygoYM/Douyu-danmu-spark/blob/master/687423_01_11_2018.jpg) and [156277_01_19_2018.jpg](https://github.com/KaygoYM/Douyu-danmu-spark/blob/master/156277_01_19_2018.jpg) (two examples).</br>
# Tips
The psd files are the templets that I use to make the final reports.</br>
Like nvliu66 and yjjimpaopao.Like 156277 and 687423 (???_??)? </br>
# Future work
Although the data may be correct generally, my scrapy process still requires further improvement since the effect and the accuracy of scrapy is worse when the amount of danmu increases suddenly. (NEEDS TO BE DONE SOON OR PROBABLY NO MORE UPDATE ( ?бу ?? ?бу) )</br>
