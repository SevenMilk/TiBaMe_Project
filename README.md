# 資策會TiBaMe AI/Bigdata數據分析師專題─扶搖職上
## 登上[TiBaMe](https://www.tibame.com/coursegoodjob/bigdata?path=works/works_JobHunter.html)官網：
* [專題簡報](https://reurl.cc/QdYx1M)
* [專題摘要](https://reurl.cc/E7VeOm)

## 負責面試捕手（模擬面試）功能，資料串接、系統服務架構設計  
![](https://witie65.echome.tw/resources?rid=8cb796cb4bfea5a350f05500ced8cb06308b94bb8648f1f3df715b6f4446d9c4&url=https%3A%2F%2Fs3-ap-northeast-1.amazonaws.com%2Fmarketing-prd%2Fgoodjob%2Fbigdata%2Fimgs%2Fworks_JobHunter_5.PNG&cid=__FGL__8624322966038f87f249433b7e2f07987d95a8b30000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000&bdk=ff4a686c02e616295ef5ae2749f02810c7c9d22be5205d3d1db3237de04eb40a&eid=2) 
* 樹莓派   
    * 蒐集user姿態、表情影像  
* views.py   
    * 樹莓派拍照程式（使用Django方法操控）  
* openpose2kafka_streaming.py   
    * 分析姿態與表情影像，且將json數據傳至kafka  
* spark_streaming.py   
    * 即時分析/計算kafka中姿態與表情json資料，並將分析結果傳回kafka  
* kafka2mysql.py  
    * 將kafka的資料送至MySQL儲存  
