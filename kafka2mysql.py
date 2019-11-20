import os,time,sys
import re,json
import pymysql
from confluent_kafka import Consumer, KafkaException, KafkaError



def consumer_json_kafka(name):

    props = {
        'bootstrap.servers': '10.120.28.129:9092',   # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'STUDENTID',                     # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'earliest',             # Offset從最前面開始
        'session.timeout.ms': 6000,                  # consumer超過6000ms沒有與kafka連線，會被認為掛掉了
        'error_cb': error_cb                         # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = name
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName], on_assign=my_assign)
    # 步驟5. 持續的拉取Kafka有進來的訊息
  
    list_data = []
    list_key = []
    msgValue=0
    try:
        while True:
            # 請求Kafka把新的訊息吐出來
            
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            #time.sleep(3)
            if len(records)==0:
                if msgValue!=0:
                    break
                else:
                    continue 
            else:
                pass
            for record in records:

                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% {} [{}] reached end at offset {} - {}\n'.format(record.topic(),
                                                                                             record.partition(),
                                                                                             record.offset()))

                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())
                    list_key.append(msgKey)
                    list_data.append(msgValue)
                    
                    #print(msgKey,msgValue)

    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(e)

    finally:
        # 步驟6.關掉Consumer實例的連線
        consumer.close()
        return list_key,list_data

    
def error_cb(err):
    print('Error: %s' % err)

# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None


# 指定要從哪個partition, offset開始讀資料
def my_assign(consumer_instance, partitions):
    for p in partitions:
        p.offset = 0
    print('assign', partitions)
    consumer_instance.assign(partitions)
 
def ETL(type,y):

    role = re.compile(r"'"+str(type)+"':\s(\d+.\d+),\s")   
 
    return role.findall(y)

def main():

    list_key,list_data=consumer_json_kafka('result2kafka')
    
    result_correct_front=json.loads(list_data[0].replace("'", '"'))
    result_error_front=json.loads(list_data[1].replace("'", '"'))
    result_error_front_count=json.loads(list_data[2].replace("'", '"'))
    
    result_correct_side=json.loads(list_data[3].replace("'", '"'))
    result_error_side=json.loads(list_data[4].replace("'", '"'))
    result_error_side_count=json.loads(list_data[5].replace("'", '"'))
    
    correct_count_face=list_data[6]
    warning_count_face=list_data[7]
    result_error_dict_face=json.loads(list_data[8].replace("'", '"'))
    
    front_count = list_data[9]
    side_count = list_data[10]

    cursor = db.cursor()

    #INSERT correct_percent
    ins_correct_percent = "insert into correct_percent values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ins_correct_percent_data = (list_key[0],result_correct_front['head_tilt_angle'],result_error_front['head_tilt_angle_left'],result_error_front['head_tilt_angle_right']\
                           ,result_correct_front['Overall_body_tilt_angle'],result_error_front['Overall_body_tilt_angle_left'],result_error_front['Overall_body_tilt_angle_right']\
                           ,result_correct_front['Shoulder_tilt_angle'],result_error_front['Shoulder_tilt_angle']\
                           ,correct_count_face,front_count\
                           ,result_correct_side['Overall_body_tilt2_angle'],result_error_side['Overall_body_tilt2_angle_left'],result_error_side['Overall_body_tilt2_angle_right']\
                           ,result_correct_side['Humpback_angle'],result_error_side['Humpback_angle']\
                           ,side_count)
                           
    cursor.execute(ins_correct_percent,ins_correct_percent_data)
    
    #INSERT face_percent
    ins_face_percent = "insert into face_percent values(%s, %s, %s, %s, %s, %s, %s, %s)"
    ins_face_percent_data = (list_key[0],result_error_dict_face['Angry'],result_error_dict_face['Disgust'],result_error_dict_face['Fear']\
                            ,result_error_dict_face['Happy'],result_error_dict_face['Sad'],result_error_dict_face['Surprise']\
                            ,result_error_dict_face['Neutral'])
                            
    cursor.execute(ins_face_percent,ins_face_percent_data)
    
    #INSERT hdfs_picture_path
    
    ins_hdfs_picture_path = "insert into hdfs_picture_path values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ins_hdfs_picture_path_data = (list_key[0]\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/front/'+result_error_front_count['head_tilt_angle_left']+'_rendered.png'\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/front/'+result_error_front_count['head_tilt_angle_right']+'_rendered.png'\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/front/'+result_error_front_count['Overall_body_tilt_angle_left']+'_rendered.png'\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/front/'+result_error_front_count['Overall_body_tilt_angle_right']+'_rendered.png'\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/front/'+result_error_front_count['Shoulder_tilt_angle']+'_rendered.png'\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/side/'+result_error_side_count['Overall_body_tilt2_angle_left']+'_rendered.png'\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/side/'+result_error_side_count['Overall_body_tilt2_angle_right']+'_rendered.png'\
                                 ,'/user/cloudera/picture/'+list_key[0]+'/working/side/'+result_error_side_count['Humpback_angle']+'_rendered.png')
                                 
    cursor.execute(ins_hdfs_picture_path,ins_hdfs_picture_path_data)
    
    db.commit()

    db.close()


if __name__ == '__main__':

    db = pymysql.connect(host='10.120.28.50',port=3306,user='root',password='root',database='wei_final')
    while True:
        main()


