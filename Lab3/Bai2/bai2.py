from pyspark import SparkContext

sc = SparkContext("local", "Bai2_PhanTichDanhGiaTheoTheLoai")

# Tạo map (MovieID -> List of Genres)
movies = sc.textFile("/home/hadoop/lab3/input/movies.txt")

movie_genres = movies.map(lambda line: line.split(",", 2)) \
    .map(lambda x: (x[0], x[2].split("|")))

movie_genres_dict = dict(movie_genres.collect())
broadcast_genres = sc.broadcast(movie_genres_dict)

# Map từ MovieID -> Rating -> (Genre, Rating)
ratings1 = sc.textFile("/home/hadoop/lab3/input/ratings_1.txt")
ratings2 = sc.textFile("/home/hadoop/lab3/input/ratings_2.txt")

ratings = ratings1.union(ratings2)

genre_ratings = ratings.map(lambda line: line.split(",")) \
    .flatMap(lambda x: [
        (genre, (float(x[2]), 1))
        for genre in broadcast_genres.value.get(x[1], [])
    ])

# Tính trung bình điểm đánh giá cho từng thể loại
genre_avg = genre_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
).mapValues(lambda x: round(x[0] / x[1], 2))

# Sắp xếp giảm dần
result = genre_avg.sortBy(lambda x: -x[1])

# Format output
header = [
    "{:<15} | {:<10}".format(
        "Genre", "AvgRating"
    ),
    "-" * 35
]

data = result.map(
    lambda x: "{:<15} | {:<10.2f}".format(
        x[0], x[1]
    )
).collect()

final_output = header + data

# Xuất file
sc.parallelize(final_output).coalesce(1).saveAsTextFile("/home/hadoop/lab3/output2")

sc.stop()
