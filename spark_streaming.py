#!/usr/bin/python
# -*- coding: UTF-8 -*-
#This version make all json for target dir at one time, and make score table (x.json one by one)

import os,time,sys
import pandas as pd
import re,json,sys
import numpy as np
from pyspark import SparkContext
#from pyspark.sql.session import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from confluent_kafka import Producer
from kafka import KafkaProducer


def angle_body(x1,x2):
    RN_4 = x1
    RR_4 = [x2,0]
    x=np.array(RN_4)
    y=np.array(RR_4)
    Lx=np.sqrt(x.dot(x))
    Ly=np.sqrt(y.dot(y))
    cos_angle=x.dot(y)/(Lx*Ly)
    angle=np.arccos(cos_angle)
    angle2=angle*360/2/np.pi
    return angle2

def judge(type_R,head_tilt_angle,Overall_body_tilt_angle,Shoulder_tilt_angle,Overall_body_tilt2_angle,Humpback_angle,count,error_dict_count,error_list_angle,error_list_angle_left,error_list_angle_right):

    correct_dict = {'head_tilt_angle':0,'Overall_body_tilt_angle':0,'Shoulder_tilt_angle':0,
            'Overall_body_tilt2_angle':0,'Humpback_angle':0}
    
    result_error_dict = {'head_tilt_angle_left':0,'head_tilt_angle_right':0,
                         'Overall_body_tilt_angle_left':0,'Overall_body_tilt_angle_right':0,
                         'Shoulder_tilt_angle':0,
                        'Overall_body_tilt2_angle_left':0,'Overall_body_tilt2_angle_right':0,
                         'Humpback_angle':0}
    
    train_left_1 = 0
    train_left_2 = 0
    train_left_4 = 0
    train_right_1 = 0
    train_right_2 = 0
    train_right_4 = 0 
    train_3 = 0
    train_5 = 0

    #init部分-----------------------------------------------------------------------------------------------
    if type_R == 1:
        if head_tilt_angle >= 80 and head_tilt_angle <= 100:
            correct_dict['head_tilt_angle'] = 1
        else:
            #分別記錄歪左右邊的最大角度照片
            if head_tilt_angle < 80: #左邊.
                train_left_1 = 80 - head_tilt_angle
                result_error_dict['head_tilt_angle_left'] = 1
                if error_list_angle_left[0] >= train_left_1:
                    pass
                else:
                    error_dict_count['head_tilt_angle_left'] = count
                    error_list_angle_left[0] = train_left_1
            elif head_tilt_angle > 100: #右邊
                train_right_1 = head_tilt_angle - 100
                result_error_dict['head_tilt_angle_right'] = 1
                if error_list_angle_right[0] >= train_right_1:
                    pass
                else:
                    error_dict_count['head_tilt_angle_right'] = count
                    error_list_angle_right[0] = train_right_1
            
        if Overall_body_tilt_angle >= 80 and Overall_body_tilt_angle <= 100:
            correct_dict['Overall_body_tilt_angle'] = 1
        else:
            if Overall_body_tilt_angle < 80: #左邊
                train_left_2 = 80 - Overall_body_tilt_angle
                result_error_dict['Overall_body_tilt_angle_left'] = 1
                if error_list_angle_left[1] >= train_left_2:
                    pass
                else:
                    error_dict_count['Overall_body_tilt_angle_left'] = count
                    error_list_angle_left[1] = train_left_2               
            elif Overall_body_tilt_angle > 100: #右邊
                train_right_2 = Overall_body_tilt_angle - 100
                result_error_dict['Overall_body_tilt_angle_right'] = 1
                if error_list_angle_right[1] >= train_right_2:
                    pass
                else:
                    error_dict_count['Overall_body_tilt_angle_right'] = count
                    error_list_angle_right[1] = train_right_2                    
                            
        if Shoulder_tilt_angle >= 0 and Shoulder_tilt_angle <= 15:
            correct_dict['Shoulder_tilt_angle'] = 1
        else:
            result_error_dict['Shoulder_tilt_angle'] = 1
            train_3 = Shoulder_tilt_angle - 15
            if error_list_angle[2] >= train_3:
                pass
            else:
                error_dict_count['Shoulder_tilt_angle'] = count
                error_list_angle[2] = train_3              
        
    elif type_R == 2:   
        if Overall_body_tilt2_angle >= 80 and Overall_body_tilt2_angle <= 90:
            correct_dict['Overall_body_tilt2_angle'] = 1
        else:
            if Overall_body_tilt2_angle < 80: #前
                train_left_4 = 80 - Overall_body_tilt2_angle
                result_error_dict['Overall_body_tilt2_angle_left'] = 1
                if error_list_angle_left[4] >= train_left_4:
                    pass
                else:
                    error_dict_count['Overall_body_tilt2_angle_left'] = count
                    error_list_angle_left[4] = train_left_4                        
            elif Overall_body_tilt2_angle > 90: #後
                train_right_4 = Overall_body_tilt2_angle - 90
                result_error_dict['Overall_body_tilt2_angle_right'] = 1
                if error_list_angle_right[4] >= train_right_4:
                    pass
                else:
                    error_dict_count['Overall_body_tilt2_angle_right'] = count
                    error_list_angle_right[4] = train_right_4                           

        if Humpback_angle >= 45:
            correct_dict['Humpback_angle'] = 1
        else:
            correct_dict['Humpback_angle'] = 0
            result_error_dict['Humpback_angle'] = 1
            train_5 = 45 - Humpback_angle
            if error_list_angle[4] >= train_5:
                pass
            else:
                error_dict_count['Humpback_angle'] = count
                error_list_angle[4] = train_5
           
    return correct_dict,error_dict_count,result_error_dict,error_list_angle,error_list_angle_left,error_list_angle_right

def analysis_front(df_front):


    #init部分-----------------------------------------------------------------------------------------------
    result_correct_dict1 = {'head_tilt_angle':0,'Overall_body_tilt_angle':0,'Shoulder_tilt_angle':0}


    result_error_dict1 ={'head_tilt_angle_left':0,'head_tilt_angle_right':0,
                         'Overall_body_tilt_angle_left':0,'Overall_body_tilt_angle_right':0,
                         'Shoulder_tilt_angle':0}

    #記錄第幾張照片最差
    error_dict_count = {'head_tilt_angle_left':'','head_tilt_angle_right':'',
                         'Overall_body_tilt_angle_left':'','Overall_body_tilt_angle_right':'',
                         'Shoulder_tilt_angle':'',}   
   
    #紀錄最差的角度
    error_list_angle = [0,0,0,0,0]
    error_list_angle_left = [0,0,0,0,0]
    error_list_angle_right = [0,0,0,0,0]
 
    # #init部分-----------------------------------------------------------------------------------------------

    for count in list_sort1:
        type_R=1
        df_nose = df_front.select("Nose").collect()[dict_sort1[count]][0].strip('[]').split(',')
        df_neck = df_front.select("Neck").collect()[dict_sort1[count]][0].strip('[]').split(',')
        df_midhip = df_front.select("MidHip").collect()[dict_sort1[count]][0].strip('[]').split(',')
        df_rshoulder = df_front.select("RShoulder").collect()[dict_sort1[count]][0].strip('[]').split(',')
        df_lshoulder = df_front.select("LShoulder").collect()[dict_sort1[count]][0].strip('[]').split(',')
                  
        #歪頭
        head_tilt_angle = angle_body([float(df_nose[0]) - float(df_neck[0]),float(df_nose[1]) - float(df_neck[1])],float(df_neck[0]))
        #整體身體傾斜
        Overall_body_tilt_angle = angle_body([float(df_nose[0]) - float(df_midhip[0]),float(df_nose[1]) - float(df_midhip[1])],float(df_midhip[0]))
        #肩膀
        Shoulder_tilt_angle = angle_body([float(df_lshoulder[0]) - float(df_rshoulder[0]),float(df_lshoulder[1]) - float(df_rshoulder[1])],float(df_rshoulder[0]))
 
        dict1,\
        result_error_dict_count,\
        error_dict1,\
        error_list_angle,error_list_angle_left,error_list_angle_right\
        = judge(type_R,head_tilt_angle,Overall_body_tilt_angle,Shoulder_tilt_angle,None,None,count,error_dict_count,error_list_angle,error_list_angle_left,error_list_angle_right)
        for key,value in result_correct_dict1.items():
            result_correct_dict1[key] += dict1[key]
        for key,value in result_error_dict1.items():
            result_error_dict1[key] += error_dict1[key]
            
    #計算正確百分比
    for key,value in result_correct_dict1.items():
        result_correct_dict1[key] = (result_correct_dict1[key] / len(list_sort1))*100
    
    #計算錯誤百分比
    for key,value in result_error_dict1.items():
        result_error_dict1[key] = (result_error_dict1[key] / len(list_sort1))*100
        
    result_list_front=[]
    result_list_front.append(result_correct_dict1)
    result_list_front.append(result_error_dict1)
    result_list_front.append(result_error_dict_count) 
    
    return result_list_front
    
def analysis_side(df_side): 

    #init部分-----------------------------------------------------------------------------------------------
    result_correct_dict2 = {'Overall_body_tilt2_angle':0,'Humpback_angle':0}

    result_error_dict2 = {'Overall_body_tilt2_angle_left':0,'Overall_body_tilt2_angle_right':0,
                         'Humpback_angle':0}
    #記錄第幾張照片最差
    error_dict_count = {'Overall_body_tilt2_angle_left':'','Overall_body_tilt2_angle_right':'','Humpback_angle':''}
   
    #紀錄最差的角度
    error_list_angle = [0,0,0,0,0]
    error_list_angle_left = [0,0,0,0,0]
    error_list_angle_right = [0,0,0,0,0]
 
    # #init部分----------------------------------------------------------------------------------------------- 

    for count in list_sort2:
    
        type_R=2
        df_nose = df_side.select("Nose").collect()[dict_sort2[count]][0].strip('[]').split(',')
        df_neck = df_side.select("Neck").collect()[dict_sort2[count]][0].strip('[]').split(',')
        df_midhip = df_side.select("MidHip").collect()[dict_sort2[count]][0].strip('[]').split(',')

        #側邊看整體身體傾斜
        Overall_body_tilt2_angle = angle_body([float(df_neck[0]) - float(df_midhip[0]),float(df_neck[1]) - float(df_midhip[1])],float(df_midhip[0]))
        #駝背
        Humpback_angle = angle_body([float(df_neck[0]) - float(df_nose[0]),float(df_neck[1]) - float(df_nose[1])],float(df_nose[0]))
        dict2,\
        result_error_dict_count,\
        error_dict2,\
        error_list_angle,error_list_angle_left,error_list_angle_right,\
        = judge(type_R,None,None,None,Overall_body_tilt2_angle,Humpback_angle,count,error_dict_count,error_list_angle,error_list_angle_left,error_list_angle_right)

        for key,value in result_correct_dict2.items():
            result_correct_dict2[key] += dict2[key]
        for key,value in result_error_dict2.items():
            result_error_dict2[key] += error_dict2[key]

    #計算正確百分比
    for key,value in result_correct_dict2.items():
        result_correct_dict2[key] = (result_correct_dict2[key] / len(list_sort2))*100

    #計算錯誤百分比
    for key,value in result_error_dict2.items():
        result_error_dict2[key] = (result_error_dict2[key] / len(list_sort2))*100
        
    result_list_side=[]
    result_list_side.append(result_correct_dict2)
    result_list_side.append(result_error_dict2)
    result_list_side.append(result_error_dict_count)       

    return result_list_side    
        
def make_dataframe(rdd):

    schema = StructType([
        StructField("Nose", StringType(), True),
        StructField("Neck", StringType(), True),
        StructField("RShoulder", StringType(), True),
        StructField("RElbow", StringType(), True),
        StructField("RWrist", StringType(), True),
        StructField("LShoulder", StringType(), True),
        StructField("LElbow", StringType(), True),
        StructField("LWrist", StringType(), True),
        StructField("MidHip", StringType(), True),
        StructField("RHip", StringType(), True),
        StructField("RKnee", StringType(), True),
        StructField("RAnkle", StringType(), True),
        StructField("LHip", StringType(), True),
        StructField("LKnee", StringType(), True),
        StructField("LAnkle", StringType(), True),
        StructField("REye", StringType(), True),
        StructField("LEye", StringType(), True),
        StructField("REar", StringType(), True),
        StructField("LEar", StringType(), True),
        StructField("LBigToe", StringType(), True),
        StructField("LSmallToe", StringType(), True),
        StructField("LHeel", StringType(), True),
        StructField("RBigToe", StringType(), True),
        StructField("RSmallToe", StringType(), True),
        StructField("RHeel", StringType(), True)
    ])  
    
    list1 = []
    list2 = []
    tuple1 = ()
    count = 0
    df_2 = spark.createDataFrame(list2,schema)
    r = rdd.map(pose_keypoints_2d).collect()
    str_data = str(r).strip('[]').replace("'","").split(',')
           
    for i in str_data:
       
        list1.append(float(i.strip()))
        count += 1
        if count == 3:
            count = 0
            list2.append(list1)
            list1 = []

    list3 = []
    list4 = []
    count = 0
    for i in list2:
        list3.append(i)
        count += 1        
        if count == 25:
            count = 0
            tuple1 = ()
            tuple1 += tuple(list3)
            list4.append(tuple1)
            list3 = []

    #[([900,200,0.8]),([]),([]),([]),.....]
    df_2 = spark.createDataFrame(list4,schema)

    return df_2

def rdd_front(rdd): 

    if len(rdd.collect()) == 0:
        pass
    else:
        global dict_sort1
        global list_sort1       
        front_data_rdd = rdd.map(lambda x: x[0]).collect()
        front_json_rdd = rdd.map(lambda x: x[1]) 

        role_ID = re.compile(r'(\w+)-')#印出 ['VG13213258']
        global ID
        ID = role_ID.findall(str(front_data_rdd))[0]
        
        role_type = re.compile(r'\w+_(\w+)')#印出照片檔名
        list_sort1 = role_type.findall(str(front_data_rdd))

        dict_sort1 = dict_sort1.fromkeys(list_sort1)
        j = 0
        for key,value in dict_sort1.items():
            dict_sort1[key] = j
            j += 1
            
        df_front = make_dataframe(front_json_rdd)
        result_list_front = analysis_front(df_front)
        return sc.parallelize(result_list_front)
        
def rdd_side(rdd): 

    if len(rdd.collect()) == 0:
        pass
    else:
        global dict_sort2
        global list_sort2        
        side_data_rdd = rdd.map(lambda x: x[0]).collect()
        side_json_rdd = rdd.map(lambda x: x[1])
        
        role_type = re.compile(r'\w+_(\w+)')#印出照片檔名
        list_sort2 = role_type.findall(str(side_data_rdd))

        dict_sort2 = dict_sort2.fromkeys(list_sort2)
        j = 0
        for key,value in dict_sort2.items():
            dict_sort2[key] = j
            j += 1
            
        df_side = make_dataframe(side_json_rdd)      
        result_list_side = analysis_side(df_side)
        return sc.parallelize(result_list_side)           
        
        #df_side.show()
                                
    
def pose_keypoints_2d(rdd):
    role = re.compile(r'pose_keypoints_2d":\[(.*)\],"face_keypoints_2d"')
    result = role.findall(rdd)
    
    return result[0]
                                   
def face_rdd(rdd):

    if len(rdd.collect()) == 0:
        pass
    else:
        result_list_face = []
        warning_count_face = {'face':0}
        correct_count_face = {'face':0}        
        face_data_rdd = rdd.map(lambda x: x[1])
   
        r = face_data_rdd.collect()
        correct_count_face['face'] = int(r[0])
        warning_count_face['face'] = int(r[1])
        error_dict_face = json.loads(r[2])       
    
        #情緒分析正面
        correct_count_face['face'] = (correct_count_face['face'] / len(list_sort1))*100
        #result_correct_dict.update(correct_count_face)
        #情緒分析負面
        warning_count_face['face'] = (warning_count_face['face'] / len(list_sort1))*100
        #result_error_dict.update(warning_count_face)
        #情緒比例
        result_error_dict_face = error_dict_face
        for key,value in result_error_dict_face.items():
            result_error_dict_face[key] = (result_error_dict_face[key] / len(list_sort1))*100            

        result_list_face.append(correct_count_face['face'])
        result_list_face.append(warning_count_face['face'])
        result_list_face.append(result_error_dict_face)
        
        return sc.parallelize(result_list_face) 

def error_cb(err):
    print('Error: %s' % err)
    
def output_rdd(rdd):    
    #rdd.foreachPartition(output_partition)
    global output_count
    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': '10.120.28.129:9092',  # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                    # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    try:
     
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        for i in rdd.collect():
            producer.produce("result2kafka", str(i), str(ID))
        if output_count == 2:
            producer.produce("result2kafka", str(len(list_sort1)), str(ID))
            producer.produce("result2kafka", str(len(list_sort2)), str(ID))
        output_count += 1
        
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    # 步驟5. 確認所在Buffer的訊息都己經送出去給Kafka了
    producer.flush()
       

def main():

    #前面
    front_stream = KafkaUtils.createStream(ssc, "localhost:2182", "consumer-group", {"json2frontkafka": 3})
    
    #側面
    side_stream = KafkaUtils.createStream(ssc, "localhost:2182", "consumer-group", {"json2sidekafka": 3}) 
    
    #情緒
    face_stream = KafkaUtils.createStream(ssc, "localhost:2182", "consumer-group", {"face2kafka": 3})

    #前面
    result_front = front_stream.transform(rdd_front)
    # result_front.pprint()
    result_front.foreachRDD(output_rdd)

    #側面
    result_side = side_stream.transform(rdd_side)
    # result_side.pprint() 
    result_side.foreachRDD(output_rdd)
    
    #情緒
    result_face = face_stream.transform(face_rdd)
    #result_face.pprint()
    result_face.foreachRDD(output_rdd)
    
    # Start it
    ssc.start()
    ssc.awaitTermination()



if __name__ == '__main__':

    try:
        sc.stop()
    except:
        pass

    spark = SparkSession \
        .builder \
        .getOrCreate()
        
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 1)
    ID = ''
    output_count = 0
    dict_sort1={}
    list_sort1=[]
    dict_sort2={}
    list_sort2=[]   
    main()

