# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:11:27 2018

@author: KAI
"""


# 这个抓取弹幕,然后把用户的id，昵称，等级，弹幕内容都保存到txt中并用spark统计
import multiprocessing
import threading
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


#chatmsg = re.compile(b'type@=chatmsg/.+?/uid@=(.+?)/nn@=(.+?)/txt@=(.+?)/.+?/level@=(\d+?)/.+?/col@=(\d+?)/.+?/bnn@=(.*?)/bl@=(.+?)/.+?')#弹幕
#chatmsg_more = re.compile(b'type@=chatmsg/.+?/uid@=(.+?)/nn@=(.+?)/txt@=(.+?)/.+?/level@=(\d+?)/.+?/col@=(\d+?)/.+?/bnn@=(.*?)/bl@=(.+?)/.+?')#弹幕
chatmsg = re.compile(b'type@=chatmsg/.+?/uid@=(.+?)/nn@=(.+?)/txt@=(.+?)/.+?/level@=(.+?)/.+?/bnn@=(.*?)/bl@=(.+?)/.+?')#弹幕
dgb=re.compile(b'type@=dgb/.+?/gfid@=(.+?)/.+?/nn@=(.+?)/.+?')#礼物


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


def starting(roomid,giftlist):
    print('---------------欢迎连接到{}的直播间---------------'.format(get_name(roomid)))
    msg = 'type@=loginreq/roomid@={}/\x00'.format(roomid)
    sendmsg(msg)
    join_room_msg = 'type@=joingroup/rid@={}/gid@=-9999/\x00'.format(roomid) #加入房间分组消息  
    sendmsg(join_room_msg)
    print("Succeed logging in")
    
    
    while True:
        danmu_file=open(str(roomid)+'_'+str(time.strftime("%d_%m_%Y"))+'.txt','a+',encoding='utf-8')
        gift_file=open(str(roomid)+'_'+str(time.strftime("%d_%m_%Y"))+'gift.txt','a+',encoding='utf-8')
        data = client.recv(2048)#bytes-like-objects
        if not data:
            print("NOT DATA")
            #print(data)
            break
        for gfid,nn in dgb.findall(data):
            if (gfid.decode() in list(giftlist.keys())[:4]):
                giftid=str(gfid.decode())
                try:
                    print("---[{}]送出----{}---".format(nn.decode(),giftlist[giftid]))#礼物部分       
                    lines=[nn.decode()+'->'+giftlist[giftid]]
                    gift_file.writelines(lines)
                    gift_file.write('\n')
                except Exception as e:
                    print(e)
                    print('-------礼物 Decode error---------')
                    pass
        #for uid,nn,txt,level,bnn,bl in chatmsg.findall(data):
        #print(data)
        try:
            for uid,nn,txt,level,bnn,bl in chatmsg.findall(data):       
                if (bl.decode()=='0'):
                    bnn=b'NONE'

                print("[{}({})][lv.{}][{}]: {} ".format(bnn.decode(),bl.decode(),level.decode(),
                      nn.decode(errors='ignore'), txt.decode(errors='ignore').strip(),))
                lines=uid.decode()+'->'+nn.decode(errors='ignore')+'->'+level.decode()+\
                    '->'+txt.decode(errors='ignore').strip()+'->'+bnn.decode()+'->'+bl.decode()               
                print(lines,file=danmu_file)
            time.sleep(0.05)
                #continue
        except Exception as e:
            print(e)
            print('-------弹幕 Decode error---------')
            
        
    danmu_file.close()
    print("成功写入TXT")


def keeplive():
    while True:
        #msg = 'type@=keeplive/tick@=' + str(int(time.time())) + '/\0'
        msg='type@=mrkl/'
        sendmsg(msg)
        print("init_live")
        time.sleep(40)


def get_name(roomid):
    r = requests.get("http://www.douyu.com/" + roomid)
    soup = BeautifulSoup(r.text, 'lxml')
    return soup.find('a', {'class', 'zb-name'}).string

def get_room_info(roomid):
    while True:
        my_file=open(str(roomid)+'_'+str(time.strftime("%d_%m_%Y"))+'room.txt','a+')
    
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        #伪装headers
        chaper_url='http://open.douyucdn.cn/api/RoomApi/room/'+str(roomid)

        req = urllib.request.Request(url=chaper_url, headers=headers)  
        page=urllib.request.urlopen(req)  
        html=page.read()
            #print(html)
            #print(type(html))
        room_info=json.loads(html.decode('utf-8'))['data']
       
        hot=room_info['hn']
        fans=room_info['fans_num']
            
        
        lines=[str(hot)+'|',str(fans)]
        my_file.writelines(lines)
        time.sleep(0.1)
        my_file.write('\n')
        print('-------room_info get-------')
        my_file.close()
        time.sleep(300)
        


if __name__ == '__main__':
    room_id = input('请出入房间ID： ')
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        #伪装headers
    chaper_url='http://open.douyucdn.cn/api/RoomApi/room/'+str(room_id)
    req = urllib.request.Request(url=chaper_url, headers=headers)  
    page=urllib.request.urlopen(req)  
    html=page.read()            
    room_info=json.loads(html.decode('utf-8'))['data']
    print('------------读取礼物列表------------')
    giftlist={}
    for each in room_info['gift']:
        giftlist[each['id']]=each['name']
        print(each['id'], giftlist[each['id']])
    p2 = threading.Thread(target=keeplive)
    p2.start()
    p3 = threading.Thread(target=get_room_info,args=(room_id,))
    p3.start()
#    while True:
    while True:
        p1 = threading.Thread(target=starting, args=(room_id,giftlist))
        p1.start()
        p1.join()
    #p1.join()
    #print("p1:",p1.is_alive())
    #p3.join()
    #p2.join()