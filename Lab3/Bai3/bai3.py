from pyspark import SparkContext

sc = SparkContext("local", "Bai3_PhanTichDanhGiaTheoGioiTinh")

# Tạo map (UserID -> Gender)
users = sc.textFile("/home/hadoop/lab3/input/users.txt")

user_gender = users.map(lambda line: line.split(",")) \
    .map(lambda x: (x[0], x[1]))

user_gender_dict = dict(user_gender.collect())
broadcast_gender = sc.broadcast(user_gender_dict)

movies = sc.textFile("/home/hadoop/lab3/input/movies.txt")

movie_titles = movies.map(lambda line: line.split(",", 2)) \
    .map(lambda x: (x[0], x[1]))

movie_titles_dict = dict(movie_titles.collect())
broadcast_titles = sc.broadcast(movie_titles_dict)

# Join với ratings để thêm thông tin giới tính
ratings1 = sc.textFile("/home/hadoop/lab3/input/ratings_1.txt")
ratings2 = sc.textFile("/home/hadoop/lab3/input/ratings_2.txt")

ratings = ratings1.union(ratings2)

ratings_with_gender = ratings.map(lambda line: line.split(",")) \
    .map(lambda x: (
        (x[1], broadcast_gender.value.get(x[0], "Unknown")),
        (float(x[2]), 1)
    ))

# Tính trung bình rating cho mỗi phim theo từng giới tính
movie_gender_stats = ratings_with_gender.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

result = movie_gender_stats.map(
    lambda x: (
        x[0][0],
        broadcast_titles.value.get(x[0][0], "Unknown"),
        x[0][1],
        round(x[1][0] / x[1][1], 2),
        x[1][1]
    )
).sortBy(lambda x: x[0])

header = [
    "{:<7} | {:<60} | {:<6} | {:<10} | {:<10}".format(
        "MovieID", "Title", "Gender", "AvgRating", "TotalRatings"
    ),
    "-" * 110
]

data = result.map(
    lambda x: "{:<7} | {:<60} | {:<6} | {:<10} | {:<10}".format(
        x[0], x[1], x[2], x[3], x[4]
    )
).collect()

final_output = header + data

sc.parallelize(final_output).coalesce(1).saveAsTextFile("/home/hadoop/lab3/output3")

sc.stop()
