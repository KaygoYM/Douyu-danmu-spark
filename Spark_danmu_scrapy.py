# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 13:59:18 2017

@author: KAI
"""

# 这个抓取弹幕,然后把用户的id，昵称，等级，弹幕内容都保存到mysql数据库中
import multiprocessing
import re
import json
import socket
import time
import urllib
import requests
from bs4 import BeautifulSoup


client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = socket.gethostbyname("openbarrage.douyutv.com")
port = 8601
client.connect((host, port))

danmu_path = re.compile(b'txt@=(.+?)/')#正则表达式
uid_path = re.compile(b'uid@=(\d+?)/')
nickname_path = re.compile(b'nn@=(.+?)/')
level_path = re.compile(b'level@=([1-9][0-9]?)/')
badgename_path = re.compile(b'bnn@=(.+?)/')
badgelevel_path=re.compile(b'bl@=(\d+?)/')
color_path=re.compile(b'col@=(\d+?)/')
vip_path=re.compile(b'nc@=(\d+?)/')

def sendmsg(msgstr):
    msg = msgstr.encode('utf-8')
    data_length = len(msg) + 8
    code = 689
    msgHead = int.to_bytes(data_length, 4, 'little') \
              + int.to_bytes(data_length, 4, 'little') + int.to_bytes(code, 4, 'little')
    client.send(msgHead)
    sent = 0
    while sent < len(msg):
        tn = client.send(msg[sent:])
        sent = sent + tn


def starting(roomid):
    print('---------------欢迎连接到{}的直播间---------------'.format(get_name(roomid)))
    msg = 'type@=loginreq/username@=rieuse/password@=douyu/roomid@={}/\0'.format(roomid)
    sendmsg(msg)
    #print(client.recv(1024))
    msg_more = 'type@=joingroup/rid@={}/gid@=-9999/\0'.format(roomid)
    sendmsg(msg_more)
    print("Succeed logging in")

    while True:
        data = client.recv(2048)#bytes-like-objects
        uid_more = uid_path.findall(data)
        nickname_more = nickname_path.findall(data)
        level_more = level_path.findall(data)
        danmu_more = danmu_path.findall(data)
        badgename_more=badgename_path.findall(data)
        badgelevel_more=badgelevel_path.findall(data)
        color_more=color_path.findall(data)
        #print('color_more',color_more)
        #print('danmu_more s length',len(danmu_more))
        #print(data)
        if not level_more:
            level_more = b'0'
        if len(color_more)<len(danmu_more):
            for _ in range(len(danmu_more)-len(color_more)):
                color_more.append(b'0')
        elif len(color_more)>len(danmu_more):
            color_more=color_more[:len(danmu_more)]
        #print('color_more s length',len(color_more))
        #print('danmu_more s length',len(danmu_more))
        if not data:
            print("NOT DATA")
            #print(data)
            break
            #continue
        else:
            for i in range(0, len(danmu_more)):
                try:
                    product={'uid':uid_more[i].decode(errors='ignore'),
                             'nickname':nickname_more[i].decode(errors='ignore'),
                             'level':level_more[i].decode(errors='ignore'),
                             'danmu':danmu_more[i].decode(errors='ignore'),
                             'badge':badgename_more[i].decode(errors='ignore'),
                             'blevel':badgelevel_more[i].decode(errors='ignore'),
                             #'gift':gift_more[i].decode(errors='ignore'),
                             #'vip_c':vip_more[i].decode(errors='ignore'),
                             'color':color_more[i].decode(errors='ignore')
                             }
                    
                    lines=[uid_more[i].decode(errors='ignore')+'|',
                           nickname_more[i].decode(errors='ignore')+'|',
                           level_more[i].decode(errors='ignore')+'|',
                           danmu_more[i].decode(errors='ignore')+'|',
                           badgename_more[i].decode(errors='ignore')+'|',
                           badgelevel_more[i].decode(errors='ignore')+'|',
                           #gift_more[i].decode(errors='ignore')+'|',
                           #vip_more[i].decode(errors='ignore')+'|',
                           color_more[i].decode(errors='ignore')
                           ]                   
                     
                    my_file=open(str(roomid)+'_'+str(time.strftime("%d_%m_%Y"))+'.txt','a+')
                    my_file.writelines(lines)
                    time.sleep(0.1)
                    my_file.write('\n')
                    print(product)
                    
                    
                except Exception as e:
                    print(e)
                    continue
    #client.close()
    my_file.close()
    print("成功写入TXT")


def keeplive():
    while True:
        #msg = 'type@=keeplive/tick@=' + str(int(time.time())) + '/\0'
        msg='type@=mrkl/'
        sendmsg(msg)
        print("init_live")
        time.sleep(10)


def get_name(roomid):
    r = requests.get("http://www.douyu.com/" + roomid)
    soup = BeautifulSoup(r.text, 'lxml')
    return soup.find('a', {'class', 'zb-name'}).string

def get_room_info(roomid,):
    while True:
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        #伪装headers
        chaper_url='http://open.douyucdn.cn/api/RoomApi/room/'+str(roomid)

        req = urllib.request.Request(url=chaper_url, headers=headers)  
        page=urllib.request.urlopen(req)  
        html=page.read()
            #print(html)
            #print(type(html))
        room_info=json.loads(html.decode('utf-8'))['data']

            #for i in room_info:
        hot=room_info['hn']
        fans=room_info['fans_num']
            #gift=json.loads(room_info['gift'])
        
        lines=[str(hot)+'|',str(fans)+'|']
        my_file=open(str(roomid)+'_'+str(time.strftime("%d_%m_%Y"))+'room_gift.txt','a+')
        my_file.writelines(lines)
        time.sleep(0.1)
        my_file.write('\n')
        print('room_info get')
        my_file.close()
        time.sleep(300)
        


if __name__ == '__main__':
    room_id = input('请出入房间ID： ')
    p2 = multiprocessing.Process(target=keeplive)
    p2.start()
    p3 = multiprocessing.Process(target=get_room_info,args=(room_id,))
    p3.start()
    while True:
        p1 = multiprocessing.Process(target=starting, args=(room_id,))
        p1.start()
        p1.join()
        print("p1:",p1.is_alive())
