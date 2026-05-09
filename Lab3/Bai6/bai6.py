from pyspark import SparkContext
from datetime import datetime

sc = SparkContext("local[*]", "Bai6_PhanTichDanhGiaTheoThoiGian")
sc.setLogLevel("ERROR")

# Đọc dữ liệu ratings
ratings1 = sc.textFile("/home/hadoop/lab3/input/ratings_1.txt")
ratings2 = sc.textFile("/home/hadoop/lab3/input/ratings_2.txt")

ratings = ratings1.union(ratings2)
ratings_rdd = ratings.map(lambda x: x.split(","))

# Chuyển Timestamp thành Year
def get_year(timestamp):
    return datetime.utcfromtimestamp(int(timestamp)).year

rating_by_year = ratings_rdd.map(
    lambda x: (
        get_year(x[3].strip()),
        (float(x[2]), 1)
    )
)

# Tính tổng lượt đánh giá và điểm trung bình
reduce_data = rating_by_year.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

result = reduce_data.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
).sortBy(lambda x: x[0])

header = [
    "{:<8} | {:<10} | {:<12}".format(
        "Year", "AvgRating", "TotalRatings"
    ),
    "-" * 38
]

data = result.map(
    lambda x: "{:<8} | {:<10.2f} | {:<12}".format(
        x[0], x[1][0], x[1][1]
    )
).collect()

final_output = header + data

sc.parallelize(final_output).coalesce(1).saveAsTextFile("/home/hadoop/lab3/output6")

sc.stop()
