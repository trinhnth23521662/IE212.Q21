from pyspark import SparkContext

sc = SparkContext("local[*]", "Bai4_PhanTichDanhGiaTheoNhomTuoi")
sc.setLogLevel("ERROR")

# Tạo map (UserID -> Age Group)
users = sc.textFile("/home/hadoop/lab3/input/users.txt")
movies = sc.textFile("/home/hadoop/lab3/input/movies.txt")
ratings1 = sc.textFile("/home/hadoop/lab3/input/ratings_1.txt")
ratings2 = sc.textFile("/home/hadoop/lab3/input/ratings_2.txt")

ratings = ratings1.union(ratings2)

users_rdd = users.map(lambda x: x.split(","))
movies_rdd = movies.map(lambda x: x.split(","))
ratings_rdd = ratings.map(lambda x: x.split(","))

def get_age_group(age):
    age = int(age)

    if age < 18:
        return "Under18"
    elif age <= 25:
        return "18-25"
    elif age <= 35:
        return "26-35"
    elif age <= 50:
        return "36-50"
    else:
        return "50+"

user_age = users_rdd.map(
    lambda x: (x[0].strip(), get_age_group(x[2].strip()))
)

movie_map = movies_rdd.map(
    lambda x: (x[0].strip(), x[1].strip())
)

# Join với ratings để thêm nhóm tuổi
rating_map = ratings_rdd.map(
    lambda x: (x[0].strip(), (x[1].strip(), float(x[2])))
)

joined = rating_map.join(user_age)

movie_age_rating = joined.map(
    lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1))
)

# Tính trung bình điểm đánh giá theo nhóm tuổi
reduce_data = movie_age_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

avg_data = reduce_data.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

formatted = avg_data.map(
    lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))
)

result = formatted.join(movie_map)

sorted_result = result.sortBy(
    lambda x: (x[0], x[1][0][0])
)

header = [
    "{:<7} | {:<60} | {:<10} | {:<10} | {:<12}".format(
        "MovieID", "Title", "AgeGroup", "AvgRating", "TotalRatings"
    ),
    "-" * 110
]

data = sorted_result.map(
    lambda x: "{:<7} | {:<60} | {:<10} | {:<10.2f} | {:<12}".format(
        x[0], x[1][1], x[1][0][0], x[1][0][1], x[1][0][2]
    )
).collect()

final_output = header + data

sc.parallelize(final_output).coalesce(1).saveAsTextFile("/home/hadoop/lab3/output4")

sc.stop()
