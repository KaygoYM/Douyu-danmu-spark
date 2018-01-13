#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 15:08:52 2017
斗鱼弹幕抓取
@author: lixiang
"""

import requests
import json

import socket
import time
import re
import os
import codecs
import sys
from termcolor import colored
from pyfiglet import Figlet
from threading import Thread

def room_info(room_id):
    url='http://open.douyucdn.cn/api/RoomApi/room/'+str(room_id)
    headers={
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10=_12_5) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.4'
            }
    print('------------房间信息获取中----------',)
    r = requests.get(url,headers=headers)
    html=r.content.decode('utf-8')
    roominfo=json.loads(html)
    if(roominfo['error']!=0):
        print('房间不存在!')
        return 1
    room_status=roominfo['data']['room_status']
    if(room_status!='1'):
        room_id=roominfo['data']['room_id']
        cate_name=roominfo['data']['cate_name']
        room_name=roominfo['data']['room_name']
        owner_name=roominfo['data']['owner_name']
        online=roominfo['data']['online']
        fans_num=roominfo['data']['fans_num']
        hot=roominfo['data']['hn']
        print('主播 '+str(owner_name)+' 未开播!',)
        print('房间名:'+str(room_name),
              '主播ID:'+str(owner_name),
              '游戏分类:'+str(cate_name),
              '在线人数:'+str(online),
              '热度:'+str(hot),
              '关注人数:'+str(fans_num))
        sys.exit(0)
        return 1
    else:
        room_id=roominfo['data']['room_id']
        cate_name=roominfo['data']['cate_name']
        room_name=roominfo['data']['room_name']
        owner_name=roominfo['data']['owner_name']
        online=roominfo['data']['online']
        fans_num=roominfo['data']['fans_num']
        hot=roominfo['data']['hn']
        #statuestitle='房间名:'+str(room_name)+'主播ID:'+str(owner_name)+'游戏分类:'+str(cate_name)+'在线人数:'+str(online)+'热度:'+str(hot)+'关注人数:'+str(fans_num)
        print('房间名:'+str(room_name),
              '主播ID:'+str(owner_name),
              '游戏分类:'+str(cate_name),
              colored('在线人数:'+str(online),'green'),
              colored('热度:'+str(hot),'red'),
              colored('关注人数:'+str(fans_num),'green'))
        print('------------读取礼物列表------------')
        global giftlist
        giftlist={}
        for each in roominfo['data']['gift']:
            giftlist[each['id']]=each['name']
            print(each['id'], giftlist[each['id']])
'''
    弹幕服务器地址  端口
    danmu.douyutv.com:8061
    anmu.douyutv.com:8062
    danmu.douyutv.com:12601
    danmu.douyutv.com:12602
    第三方接入弹幕服务器列表
    IP 地址：openbarrage.douyutv.com
    端口：8601
'''
client= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
Host='openbarrage.douyutv.com' 
Port=8601
client.connect((Host,Port))

def sendmsg(msgstr):
    msg=msgstr.encode('utf-8')
    data_length= len(msg)+8
    code=689
    msgHead = int.to_bytes(data_length, 4, 'little') + int.to_bytes(data_length, 4, 'little') + int.to_bytes(code, 4, 'little')
    client.send(msgHead)  #发送协议头
    client.send(msg)      #发送消息请求

def connectdanmuserver(room_id,opengift='1',enterroom='1',opentxt='1'):
    d = os.path.dirname(__file__)# 获取当前文件路径
    isexist(d+'/弹幕'+'//'+str(room_id)+'danmuass.txt')
    if(room_info(room_id)==1):
         return 0
    if (opentxt=='1' or opentxt==''):
        print(colored('-------------弹幕已开启------------','red'))
    if (opengift=='1' or opengift==''):
        print(colored('-------------礼物已开启------------','red'))
    if (enterroom=='1' or enterroom=='' ):
        print(colored('------------进房提醒已开启------------','red'))
    print('------------弹幕服务器连接中----------',)
    msg = 'type@=loginreq/roomid@={}/\x00'.format(room_id)
    sendmsg(msg)
    join_room_msg = 'type@=joingroup/rid@={}/gid@=-9999/\x00'.format(room_id) #加入房间分组消息  
    sendmsg(join_room_msg)  
    chatmsg = re.compile(b'type@=chatmsg/.+?/nn@=(.+?)/txt@=(.+?)/.+?/level@=(.+?)/.+?/bnn@=(.+?)/bl@=(.+?)/.+?')
    dgb=re.compile(b'type@=dgb/.+?/gfid@=(.+?)/.+?/nn@=(.+?)/.+?')
    uenter=re.compile(b'type@=uenter/.+?/nn@=(.+?)/.+?')
    with codecs.open(d+'/speciallist.txt', 'a+', 'utf-8') as f:
        speciallist = f.read()
        print (speciallist)
        
    time_start=time.time()
    while True:  
        data = client.recv(1024)  #这个data就是服务器向客户端发送的消息
        outputass = open(d+'/弹幕'+'//'+str(room_id)+'danmuass.txt', 'a+')
        output = open(d+'/弹幕'+'//'+str(room_id)+'danmu.txt', 'a+')
        #print(data)
        #print(chatmsg.findall(data))
        #print(dgb.findall(data))
        if (enterroom=='1' or enterroom==''):
            for nn in uenter.findall(data):
                    try:
                        if(nn.decode() in speciallist):
                            print(colored("---【{}】进入房间---".format(nn.decode()),'green','on_blue',attrs=['reverse', 'blink','bold']))
                        else:
                            print(colored("---【{}】进入房间---".format(nn.decode()),'green','on_blue'))
    
                    except:
                        print('-------进房 Decode error---------')
                        pass
        if (opengift=='1' or opengift==''):  
            for gfid,nn in dgb.findall(data):
                if (gfid.decode() in giftlist):
                    giftid=str(gfid.decode())
                    try:
                        print(colored("---[{}]送出----{}---".format(nn.decode(),giftlist[giftid]),'white','on_red'))
                    except:
                        print('-------礼物 Decode error---------')
                        pass
       
        if (opentxt=='1' or opentxt==''):
            for nn,txt,level,bnn,bl in chatmsg.findall(data):
                try:
                    if(nn.decode() in speciallist):
                        print(colored('---------------------------------------','green'))
                    if (bl.decode()!='0'):
                        if(nn.decode() in speciallist):
                            print(colored("[{}({})][lv.{}][{}]: {} ".format(bnn.decode(),bl.decode(),level.decode(),nn.decode(), txt.decode().strip(),),'white','on_magenta',attrs=['blink','bold']))
                        else:
                            print("[{}({})][lv.{}][{}]: {} ".format(bnn.decode(),bl.decode(),level.decode(),nn.decode(), txt.decode().strip(),))
                    else:
                        print("[lv.{}][{}]: {} ".format(level.decode(),nn.decode(), txt.decode().strip(),))
                #print("[lv.{}][{}]: {} [{}]".format(level.decode(), nn.decode(), txt.decode().strip(), time.strftime("%Y-%m-%d %H:%M:%S")))
                    outputass.write("{}|{}\n".format(time.time()-time_start,txt.decode().strip()))
                    if(nn.decode() in speciallist):
                        print(colored('---------------------------------------','green'))
                    print("{}:{}".format( nn.decode(), txt.decode().strip()),file=output)
                #output.write(txt.decode().strip()+'\n')
                #output.close()
                except:
                #print('-------弹幕 Decode error---------')
                    pass
            #UnicodeDecodeError as e:   #斗鱼有些表情会引发unicode编码错误
            #print(e)
            
def keeplive():
    while True:
        #msg='type@=keeplive/tick@={}/\x00'.format(int(time.time()))
        msg = 'type@=mrkl/\x00'
        sendmsg(msg)
        time.sleep(10)
def isexist(path):
    isExists=os.path.exists(path)
    if (isExists==True):
        print('删除原弹幕文件')
        os.remove(path)
        
def titleconvert(strfont):
    font=strfont
    f = Figlet(font='stop')
    print (colored(f.renderText(font),'green',attrs=['bold']))

if __name__ == '__main__':
    titleconvert('Douyu')
    titleconvert('Danmu')
    titleconvert(' V 2.0')
    room_id = input(colored('请输入房间号:','green',attrs=['bold']))
    opentxt=input('是否开启弹幕(默认1 能 0 不能):')
    opengift=input('是否开启礼物(默认1 能 0 不能):')
    enterroom=input('是否开启进房提醒(默认1 能 0 不能):')
    t1 = Thread(target=connectdanmuserver,args=(room_id,opengift,enterroom,opentxt))
    t2 = Thread(target=keeplive)
    t1.start()
    t2.start()   
    #wordcloud(room_id)

