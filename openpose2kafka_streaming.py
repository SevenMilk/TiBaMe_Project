#!/usr/bin/python3.7
# -*- coding: UTF-8 -*-
#This version make all json for target dir at one time, and make score table (x.json one by one) 

from confluent_kafka import Producer
from hdfs import InsecureClient
import os,time,sys
import pandas as pd
import re,json,sys
import numpy as np

import matplotlib.pyplot as plt
from PIL import Image
import torch
import torch.nn as nn
import torch.nn.functional as F
import os,re,sys
from torch.autograd import Variable

sys.path.append("./pytorch/")
import transforms as transforms
from skimage import io
from skimage.transform import resize
from models import *

cut_size = 44

transform_test = transforms.Compose([
    transforms.TenCrop(cut_size),
    transforms.Lambda(lambda crops: torch.stack([transforms.ToTensor()(crop) for crop in crops])),
])


def transform(): #一次解析全部所有images
    
    os.system('mv ./picture/'+ID+'/waiting/front/*.png ./picture/'+ID+'/working/front/')
    os.system('mv ./picture/'+ID+'/waiting/side/*.png ./picture/'+ID+'/working/side/')
    try: #目前還沒遇過一個dir中有圖片有問題的，openpose如果遇到解析有問題應該就會停下來  
        command_front = './build/examples/openpose/openpose.bin --image_dir ./picture/'+ID+'/working/front/ --write_json ./picture/'+ID+'/working/front/ --write_images ./picture/'+ID+'/working/front/ --display 0 --net_resolution 48x48'
        command_side = './build/examples/openpose/openpose.bin --image_dir ./picture/'+ID+'/working/side/ --write_json ./picture/'+ID+'/working/side/ --write_images ./picture/'+ID+'/working/side/ --display 0 --net_resolution 48x48'
        os.popen(command_front).read()
        os.popen(command_side).read()
    except:
        pass

def check_make_json_front():
    
    r1 = os.popen(r'ls -rt ./picture/'+ID+'/working/front/*_rendered.png').read().split('\n')
    yy = re.compile(r'front/(\d+)_rendered.png')
    list_sort = yy.findall(str(r1))
    
    for count in list_sort:
        #第一步：檢查每張照片的json檔是否正常產出(一張一張檢查) 
        if os.system('ls ./picture/'+ID+'/working/front/'+str(count)+'_keypoints.json') == 0:
            #第二步：檢查json檔中是否有'people' key, 檢查json檔是否偵測到二人以上的數值
            data = open(r'./picture/'+ID+'/working/front/'+str(count)+'_keypoints.json',encoding='utf-8').read()
            jsonStr = json.loads(data)#python資料結構(一般為字典)轉換為JSON編碼的字串
            rule = re.compile(r'pose_keypoints_2d')
            target = rule.findall(str(jsonStr))
            if target:
                if len(target) > 1:
                    # openpose偵測到有第二個人存在
                    os.system('mv ./picture/'+ID+'/working/front/'+str(count)+'_keypoints.json ./picture/'+ID+'/error/front/')
                    os.system('mv ./picture/'+ID+'/working/front/'+str(count)+'_rendered.png ./picture/'+ID+'/error/front/')    
                else:
                    pass
            else:
                os.system('mv ./picture/'+ID+'/working/front/'+str(count)+'_keypoints.json ./picture/'+ID+'/error/front/')
                os.system('mv ./picture/'+ID+'/working/front/'+str(count)+'_rendered.png ./picture/'+ID+'/error/front/')   
        else: 
            os.system('mv ./picture/'+ID+'/working/front/'+str(count)+'_rendered.png ./picture/'+ID+'/error/front/')

def check_make_json_side():
    
    r1 = os.popen(r'ls -rt ./picture/'+ID+'/working/side/*_rendered.png').read().split('\n')
    yy = re.compile(r'side/(\d+)_rendered.png')
    list_sort = yy.findall(str(r1))      
    
    for count in list_sort:
        #第一步：檢查每張照片的json檔是否正常產出(一張一張檢查) 
        if os.system('ls ./picture/'+ID+'/working/side/'+str(count)+'_keypoints.json') == 0:
            #第二步：檢查json檔中是否有'people' key, 檢查json檔是否偵測到二人以上的數值
            data = open(r'./picture/'+ID+'/working/side/'+str(count)+'_keypoints.json',encoding='utf-8').read()
            jsonStr = json.loads(data)#python資料結構(一般為字典)轉換為JSON編碼的字串
            rule = re.compile(r'pose_keypoints_2d')
            target = rule.findall(str(jsonStr))
            if target:
                if len(target) > 1:
                    # openpose偵測到有第二個人存在
                    os.system('mv ./picture/'+ID+'/working/side/'+str(count)+'_keypoints.json ./picture/'+ID+'/error/side/')
                    os.system('mv ./picture/'+ID+'/working/side/'+str(count)+'_rendered.png ./picture/'+ID+'/error/side/')    
                else:
                    pass
            else:
                os.system('mv ./picture/'+ID+'/working/side/'+str(count)+'_keypoints.json ./picture/'+ID+'/error/side/')
                os.system('mv ./picture/'+ID+'/working/side/'+str(count)+'_rendered.png ./picture/'+ID+'/error/side/')   
        else: 
            os.system('mv ./picture/'+ID+'/working/side/'+str(count)+'_rendered.png ./picture/'+ID+'/error/side/')                     
    
def rgb2gray(rgb):
    return np.dot(rgb[...,:3], [0.299, 0.587, 0.114])

def face_func(list_sort):
    
    error_dict_face = {'Angry':0, 'Disgust':0, 'Fear':0, 'Happy':0, 'Sad':0, 'Surprise':0, 'Neutral':0}
    warning_list = ['Angry', 'Disgust', 'Fear', 'Sad']
    correct_parameter = 0
    warningcount_face = 0
    for count in list_sort:
        raw_img = io.imread('./picture/'+ID+'/working/front/'+str(count)+'.png')
        gray = rgb2gray(raw_img)
        gray = resize(gray, (48,48), mode='symmetric').astype(np.uint8)

        img = gray[:, :, np.newaxis]

        img = np.concatenate((img, img, img), axis=2)
        img = Image.fromarray(img)
        inputs = transform_test(img)

        class_names = ['Angry', 'Disgust', 'Fear', 'Happy', 'Sad', 'Surprise', 'Neutral']

        net = VGG('VGG19')
        checkpoint = torch.load(os.path.join('./pytorch/FER2013_VGG19', 'PrivateTest_model.t7'))
        net.load_state_dict(checkpoint['net'])
        net.cuda()
        net.eval()

        ncrops, c, h, w = np.shape(inputs)

        inputs = inputs.view(-1, c, h, w)
        inputs = inputs.cuda()
        inputs = Variable(inputs, volatile=True)
        outputs = net(inputs)

        outputs_avg = outputs.view(ncrops, -1).mean(0)  # avg over crops

        score = F.softmax(outputs_avg)
        _, predicted = torch.max(outputs_avg.data, 0)
        face = str(class_names[int(predicted.cpu().numpy())])
        
        error_dict_face[face] += 1
        if face in warning_list:
            warningcount_face += 1
        else:
            correct_parameter += 1
    
    return correct_parameter,warningcount_face,error_dict_face

# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


def main():

    warning_count_face = {'face':0}
    correct_count_face = {'face':0}
    
    #transform()
    check_make_json_front()
    check_make_json_side()
    
    r1 = os.popen(r'ls -rt ./picture/'+ID+'/working/front/*_rendered.png').read().split('\n')
    r2 = os.popen(r'ls -rt ./picture/'+ID+'/working/side/*_rendered.png').read().split('\n')
    #yy = re.compile(r'(\d+)')
    #list_sort1 = yy.findall(str(r1))#正面
    #list_sort2 = yy.findall(str(r2))#側面    

    yy1 = re.compile(r'/front/(\d+)')
    yy2 = re.compile(r'/side/(\d+)')
    list_sort1 = yy1.findall(str(r1))#正面
    list_sort2 = yy2.findall(str(r2))#側面   
    #情緒分析
    correct_count_face['face'] , warning_count_face['face'] , error_dict_face = face_func(list_sort1)

    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': '10.120.28.129:9092',  # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                    # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    #topicName = 'json2kafka'
    #ID = 'VG13213258'# 之後要從Line存取
    try:
     
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        
        #傳送情緒資料
        producer.produce(topicName, str(correct_count_face['face']), str(ID)+'_correct_count_face_')
        producer.produce(topicName, str(warning_count_face['face']), str(ID)+'_warning_count_face_')
        producer.produce(topicName, json.dumps(error_dict_face), str(ID)+'_error_dict_face_')     
        
        # 傳送正面json
        for i in list_sort1:
            with open( './picture/'+ID+'/working/front/'+str(i)+'_keypoints.json', 'r' ) as f:
                #F: 前面 S:後面
                #f.read() = value
                json_value = f.read()
                producer.produce(topicName, json_value, str(ID)+'_Front_'+str(i))

        # 傳送測面json
        for i in list_sort2:
            with open( './picture/'+ID+'/working/side/'+str(i)+'_keypoints.json', 'r' ) as f:
                json_value = f.read()
                producer.produce(topicName, json_value, str(ID)+'_Side_'+str(i))
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    # 步驟5. 確認所在Buffer的訊息都己經送出去給Kafka了
    producer.flush()


    ## 火柴人圖片丟到HDFS
    con_hdfs = InsecureClient(url="http://10.120.28.129:50070",user='cloudera')
    try:
        con_hdfs.makedirs(ID+'/front_png')
        con_hdfs.makedirs(ID+'/side_png')
        for i in list_sort1:
            con_hdfs.upload(ID+'/front_png','./picture/'+ID+'/working/front/'+str(i)+'_rendered.png')
        for i in list_sort2:
            con_hdfs.upload(ID+'/side_png','./picture/'+ID+'/working/side/'+str(i)+'_rendered.png')
    except:
        pass
    
        
   
              
if __name__ == '__main__':

    topicName = 'json2kafka'
    ID = 'VG13213258'
  
    while True:
        while True:
            if os.system('ls ./picture/'+ID+'/waiting/front/*.png') == 0:
                break
            else:
                time.sleep(1)            
        main()
        os.system('mv ./picture/'+ID+'/working/front/*_rendered.png ./picture/'+ID+'/finish/front/')        
        os.system('mv ./picture/'+ID+'/working/front/*_keypoints.json ./picture/'+ID+'/finish/front/')
        os.system('mv ./picture/'+ID+'/working/front/*.png ./picture/'+ID+'/origin/front/')
        os.system('mv ./picture/'+ID+'/working/side/*_rendered.png ./picture/'+ID+'/finish/side/')        
        os.system('mv ./picture/'+ID+'/working/side/*_keypoints.json ./picture/'+ID+'/finish/side/')
        os.system('mv ./picture/'+ID+'/working/side/*.png ./picture/'+ID+'/origin/side/')
       
        
        
        
        
        
