# Yêu cầu 
## Log quảng cáo: AdnLog
Log này là log quảng cáo có chưa một số thông tin cơ bản sau:
- Guid: định danh user
- CampaignID: ID chiến dich
- BannerID
- ClickOrView: false là view
Y/C Hãy xây dựng một server có thể trả dữ liệu số user view/click đối với một campaign hoặc banner theo thời gian cho trước, từ ngày A -> ngày B. Thời gian phản hồi không được phép quá 1 min.

# Các bước thực hiện
## Khởi chạy spark-shell
```
/data/spark-3.4.3/bin/spark-shell --master yarn --deploy-mode client --num-executors 3 --executor-memory 1G
```
## Tiền xử lý dữ liệu từ server
- Lấy mẫu với dữ liệu campaign
```scala
{
  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, SparkSession}

  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val basePath = new Path("hdfs://adt-platform-dev-106-254:8120/data/Parquet/AdnLog")

  // Lấy các thư mục ngày (bỏ thư mục ẩn)
  val dayDirs = fs.listStatus(basePath)
    .filter(status => status.isDirectory && !status.getPath.getName.startsWith("."))
    .map(_.getPath)

  // Hàm xử lý từng ngày, trả về Option[DataFrame]
  def processDay(dayPath: Path): Option[DataFrame] = {
    val dateStr = dayPath.getName
	val formattedDate = dateStr.replace('_', '-')
    val parquetFiles = fs.listStatus(dayPath)
      .map(_.getPath.toString)
      .filter(_.endsWith(".parquet"))

    if (parquetFiles.isEmpty) return None

    try {
      val df = spark.read.parquet(parquetFiles: _*)
        .select($"guid", $"campaignId", $"click_or_view")
        .distinct()
        .withColumn("Date", lit(formattedDate))

      val campaignDF = df
        .filter($"campaignId".isNotNull && $"campaignId" =!= -1 && $"guid" =!= -1)
        .select($"guid", $"campaignId", $"click_or_view", $"Date")

      Some(campaignDF)

    } catch {
      case e: Exception =>
        println(s"⚠ Lỗi xử lý ngày $formattedDate: ${e.getMessage}")
        None
    }
  }

  // Xử lý toàn bộ ngày
  val allResults = dayDirs.map(processDay)
  val campaignDFs = allResults.flatten

  val finalcampaignDF = if (campaignDFs.isEmpty) spark.emptyDataFrame else campaignDFs.reduce(_ union _)

  // Ghi kết quả ra HDFS
  if (!finalcampaignDF.isEmpty) {
    finalcampaignDF
      .write
      .mode("overwrite")
      .csv("hdfs://adt-platform-dev-106-254:8120/user/minhnn/final_campaign_output6")
    println("✅ Đã ghi Campaign ra HDFS.")
  } else {
    println("🚫 Không có dữ liệu Campaign.")
  }
}
```
- Sau khi lưu các file csv vào thư mục "/user/minhnn/final_campaign_output6", lưu tiếp chúng về "home/minhnn"
```bash
hdfs dfs -ls hdfs://adt-platform-dev-106-254:8120/user/minhnn/final_campaign_output6
hdfs dfs -get hdfs://adt-platform-dev-106-254:8120/user/minhnn/final_campaign_output6/part-00000-b6e6b721-3328-4130-b114-3ac9e07406c1-c000.csv ~/
```
- Tiếp tục lưu chúng về máy tính cá nhân bằng cách mở powershell và chạy lệnh
```bash
tsh scp minhnn@adt-platform-hbase-dev-106-254:/home/minhnn/part-00000-b6e6b721-3328-4130-b114-3ac9e07406c1-c000.csv ./Downloads/dataforbanner/
```
## Lưu dữ liệu vào CSDL
- Sử dụng clickhosue và docker để triển khai CSDL
- Tạo container cho CSDL
```bash
docker run -d --name clickhouse-server -e CLICKHOUSE_DB=default -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD=123456 -v "C:/Users/LEGION PC/Downloads/dataforbanner:/data" -p 8123:8123 -p 9000:9000 -p 9009:9009 clickhouse/clickhouse-server
```
- Lưu ý: Lệnh này đang mount trực tiếp nơi lưu trữ dữ liệu ở máy local cụ thể là trong "C:/Users/LEGION PC/Downloads/dataforbanner/", có thể tùy chỉnh cấu hình này
- Sau khi tạo thành công, tạo bảng dữ liệu tương ứng, ví dụ ở đây tạo bảng cho campaign
```sql
CREATE TABLE adnlog_raw_campaign
(
    guid Int64,
    campaign_id Int32,
    click_or_view String,
    event_date Date
)
ENGINE = MergeTree
ORDER BY (event_date, campaign_id, guid)
```
- Sau đó vào terminal để insert dữ liệu bằng lệnh:
```bash
$files = Get-ChildItem "C:\Users\LEGION PC\Downloads\dataforbanner" -Filter *.csv
foreach ($file in $files) {
    $filePath = $file.FullName
    Write-Host "Importing: $filePath"
    $dockerCmd = "type `"$filePath`" | docker exec -i clickhouse-server clickhouse-client --password 123456 --query=`"INSERT INTO adnlog_raw_campaign FORMAT CSV`""
    cmd /c $dockerCmd
}
```
## Phương pháp dùng thuật toán HLL để phục vụ truy vấn
### Nguyên lý của thuật toán HyperLogLog
**1. Sử dụng hàm băm với mỗi phần từ chuyển thành chuỗi nhị phân**
   
Mỗi phần tử ta sẽ sử dụng một hàm băm, gán cho phần tử một giá trị giả lập và chuyển về chuỗi nhị phân.

VD: 'A' gán giá trị là 18 và chuyển thành chuỗi nhị phân là ```00011000``` nếu ta dùng hàm băm 8bit.

**2. Đếm số lượng bit 0 liên tiếp từ trái qua phải (leading zeros)**

Sau khi có chuỗi giá trị nhị phân của từng phân tử, ta thực hiện đếm số lượng bit 0 ở đầu gọi là leading zeros

VD: ```00011000``` sẽ có leading zeros là 3 

**3. Chia nhỏ thành nhiều bucket**

Ta lấy p bit đầu tiên của hash để xác định bucket số m, khi đó:

<img width="326" height="60" alt="image" src="https://github.com/user-attachments/assets/51b48db1-4129-4ab4-8a92-6281cd2746f1" />

VD: ta lấy số bit p = 2 thì số bucket m = 4 (00, 01, 10, 11) 

```00011000``` có 2 ký tự đầu là 00 (bucket 0)

Bỏ 2 ký tự đầu đi ta còn lại ```011000``` khi đó giá trị mới sẽ có leading zeros là 1.

Tương tự với các phần tử khác t sẽ được tập giá trị leading zeros của mỗi bucket, sau đó ta sẽ lấy giá trị lớn nhất (max) của mỗi bucket để tính toán. 

Bucket rỗng thì giá trị tính toán sẽ là 0. 

**4. Dùng công thức tính trung bình với hệ số hiệu chỉnh (phụ thuộc vào số bucket)**

<img width="410" height="138" alt="image" src="https://github.com/user-attachments/assets/b5563507-8e85-4e89-8bd3-45d02abbb3e3" />

Trong đó hệ số alpha sẽ có công thức tính riêng nhưng ta thường dùng một vài giá trị có sẵn để tính toán. Nếu số lượng bucket quá lớn thì sẽ dùng công thức để tính xấp xỉ.

<img width="250" height="89" alt="image" src="https://github.com/user-attachments/assets/1004f598-3dfe-48d7-8a76-0e9e3ee19b0b" />

## Tạo app
-Sử dụng python
```python
from fastapi import FastAPI, Query, HTTPException
from clickhouse_connect import get_client
from datetime import date
from typing import Optional

app = FastAPI()
def get_clickhouse_client():
    try:
        client = get_client(
            host="localhost",
            port=8123,
            user="default",
            password="123456",
            database="default"
        )
        return client
    except Exception as e:
        print("⚠️ Không kết nối được CSDL:", e)
        return None

@app.get("/countforbanner")
def get_estimated_users(
    start_date: Optional[date] = Query(..., description="YYYY-MM-DD"),
    end_date: Optional[date] = Query(..., description="YYYY-MM-DD")
):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE event_date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        where_clause = f"WHERE event_date = '{start_date}'"
    elif end_date:
        where_clause = f"WHERE event_date = '{end_date}'"
    query = f"""
        SELECT banner_id, click_or_view, uniqHLL12(guid) AS estimated_user_count
        FROM adnlog_raw_v2
        {where_clause}
        GROUP BY banner_id, click_or_view
        ORDER BY banner_id
    """
    client = get_clickhouse_client()
    result = client.query(query)
    return result.result_rows

@app.get("/countforcampaign")
def get_estimated_users(
    start_date: Optional[date] = Query(..., description="YYYY-MM-DD"),
    end_date: Optional[date] = Query(..., description="YYYY-MM-DD")
):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE event_date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        where_clause = f"WHERE event_date = '{start_date}'"
    elif end_date:
        where_clause = f"WHERE event_date = '{end_date}'"
    query = f"""
        SELECT campaign_id, click_or_view, uniqHLL12(guid) AS estimated_user_count
        FROM adnlog_raw_campaign
        {where_clause}
        GROUP BY campaign_id, click_or_view
        ORDER BY campaign_id
    """
    client = get_clickhouse_client()
    result = client.query(query)
    return result.result_rows
```
- Để chạy app, vào terminal và chạy lệnh
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```
## Hướng dẫn sử dụng api
### 3. API đếm số lượng view/click cho campaign

#### 3.1 Mục đích

Truy vấn số lượng người dùng ước tính theo `campaign_id`.

#### 3.2 Input

| Tên tham số | Kiểu     | Bắt buộc | Mô tả                          |
|-------------|----------|----------|---------------------------------|
| `start_date`| `date`   | Không    | Ngày bắt đầu (YYYY-MM-DD)      |
| `end_date`  | `date`   | Không    | Ngày kết thúc (YYYY-MM-DD)     |

- Nếu truyền cả hai: lọc theo `BETWEEN`.
- Nếu chỉ truyền 1 trong 2: lọc bằng đúng ngày đó.
- Nếu không truyền gì: không lọc theo ngày.

### 3.3 Ví dụ gọi API
- http://localhost:8000/countforcampaign?start_date=2024-12-11&end_date=2024-12-13
### 3.4 Output
| Tên trường             | Kiểu dữ liệu | Mô tả                                                                 |
|------------------------|--------------|-----------------------------------------------------------------------|
| `campaign_id`          | `int`        | ID của chiến dịch (chỉ có ở `/countforcampaign`)                     |
| `click_or_view`        | `int`        | Phân loại hành vi: `0` là **view**, `1` là **click**                 |
| `estimated_user_count` | `int`        | Số lượng người dùng ước tính (sử dụng `uniqHLL12(guid)` trong ClickHouse) |
