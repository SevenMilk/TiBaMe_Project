# 資策會AI/Bigdata數據分析師專題─扶搖職上
專題簡報：https://drive.google.com/open?id=1ElYZUH3oBinIgs6aK76vdBrjcNetkWHJ  
專題摘要：https://drive.google.com/open?id=1zSrs4actYfLGhW29NsKVJxO359ENWPDu  
面試捕手（模擬面試）資料串接

樹莓派：蒐集user姿態、表情影像  
views.py：樹莓派拍照程式（使用Django方法操控）  
openpose2kafka_streaming.py：分析姿態與表情影像，且將json數據傳至kafka  
spark_streaming.py：即時分析/計算kafka中姿態與表情json資料，並將分析結果傳回kafka  
kafka2mysql.py：將kafka的資料送至MySQL儲存  
