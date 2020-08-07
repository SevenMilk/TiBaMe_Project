![](https://github.com/SevenMilk/TiBaMe_Project/blob/master/images/%E5%B0%81%E9%9D%A2.png)
## 登上[TiBaMe](https://www.tibame.com/coursegoodjob/bigdata?path=works/works_JobHunter.html)官網：
* [專題簡報](https://reurl.cc/QdYx1M)
* [專題摘要](https://reurl.cc/E7VeOm)

### 工作說明
* 負責面試捕手（模擬面試）功能，資料串接、系統服務架構設計  
![](https://github.com/SevenMilk/TiBaMe_Project/blob/master/images/%E6%9E%B6%E6%A7%8B%E5%9C%96.png)
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
