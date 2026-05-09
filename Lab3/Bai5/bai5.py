from pyspark import SparkContext

sc = SparkContext("local[*]", "Bai5_PhanTichDanhGiaTheoOccupation")
sc.setLogLevel("ERROR")

# Tạo dictionary UserID -> Occupation
users = sc.textFile("/home/hadoop/lab3/input/users.txt")
ratings1 = sc.textFile("/home/hadoop/lab3/input/ratings_1.txt")
ratings2 = sc.textFile("/home/hadoop/lab3/input/ratings_2.txt")
occupations = sc.textFile("/home/hadoop/lab3/input/occupation.txt")

ratings = ratings1.union(ratings2)

users_rdd = users.map(lambda x: x.split(","))
ratings_rdd = ratings.map(lambda x: x.split(","))
occupation_rdd = occupations.map(lambda x: x.split(","))

occupation_map = dict(
    occupation_rdd.map(lambda x: (x[0].strip(), x[1].strip())).collect()
)

user_occupation = users_rdd.map(
    lambda x: (
        x[0].strip(),
        (x[3].strip(), occupation_map.get(x[3].strip(), "Unknown"))
    )
)

occupation_dict = dict(user_occupation.collect())
broadcast_occupation = sc.broadcast(occupation_dict)

# Gán Occupation theo UserID
rating_with_occupation = ratings_rdd.map(
    lambda x: (
        broadcast_occupation.value.get(
            x[0].strip(),
            ("Unknown", "Unknown")
        ),
        (float(x[2]), 1)
    )
)

# Tính trung bình rating và tổng số lượt đánh giá
reduce_data = rating_with_occupation.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

result = reduce_data.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
).sortBy(lambda x: (-x[1][0], int(x[0][0])))

header = [
    "{:<5} | {:<20} | {:<10} | {:<12}".format(
        "ID", "Occupation", "AvgRating", "TotalRatings"
    ),
    "-" * 75
]

data = result.map(
    lambda x: "{:<5} | {:<20} | {:<10.2f} | {:<12}".format(
        x[0][0], x[0][1], x[1][0], x[1][1]
    )
).collect()

final_output = header + data

sc.parallelize(final_output).coalesce(1).saveAsTextFile("/home/hadoop/lab3/output5")

sc.stop()
