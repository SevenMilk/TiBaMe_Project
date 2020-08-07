![](https://github.com/SevenMilk/TiBaMe_Project/blob/master/images/%E5%B0%81%E9%9D%A2.png)
## 登上[TiBaMe](https://www.tibame.com/coursegoodjob/bigdata?path=works/works_JobHunter.html)官網：
* [專題簡報](https://reurl.cc/QdYx1M)
* [專題摘要](https://reurl.cc/E7VeOm)

### 專題目標 幫助使用者找好方向、寫好履歷、做好面試
* 在人力資源網上的職缺多如牛毛，描述不一..
   * 本作品使用LineChatbot，彙整並分析各人力資源網上的重要資訊。簡單幾個鍵，協助新鮮人的生涯決策。
* 履歷是開啟職業生涯的敲門磚
   * 可惜的是，新鮮人會犯下許多常見的錯誤。本作品將在履歷的照片上，給予選擇的建議。
* 如何面試的時候大放異彩
   * 你所表現的儀態，其實是影響結果的重要因素。本作品將錄製你的面是模樣，進行儀態和表情評估，提供建議。


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
