from pyspark import SparkContext

sc = SparkContext("local", "Bai1_TinhDiemTrungBinhVaSoLuotDanhGia")

# Đọc file movies.txt và tạo map (MovieID -> Title)
movies = sc.textFile("/home/hadoop/lab3/input/movies.txt")

movie_map = movies.map(lambda x: x.split(",")) \
    .map(lambda x: (x[0].strip(), x[1].strip()))

# Đọc file ratings_1.txt và ratings_2.txt, map MovieID -> (Rating, 1)
ratings1 = sc.textFile("/home/hadoop/lab3/input/ratings_1.txt")
ratings2 = sc.textFile("/home/hadoop/lab3/input/ratings_2.txt")

ratings = ratings1.union(ratings2)

rating_map = ratings.map(lambda x: x.split(",")) \
    .map(lambda x: (x[1].strip(), (float(x[2]), 1)))

# Reduce để tính tổng điểm và số lượt đánh giá
rating_reduce = rating_map.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# Tính điểm trung bình, lọc ra phim có ít nhất 5 lượt đánh giá
avg_rating = rating_reduce.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

result = avg_rating.join(movie_map)

final = result.map(
    lambda x: (
        x[0],
        x[1][1],
        x[1][0][0],
        x[1][0][1]
    )
)

sorted_result = final.sortBy(lambda x: -x[2])

all_movies = sorted_result.collect()

movies_5 = final.filter(lambda x: x[3] >= 5).collect()
movies_50 = final.filter(lambda x: x[3] >= 50).collect()

# Tìm phim có điểm trung bình cao nhất
header = [
    "{:<7} | {:<60} | {:<10} | {:<12}".format(
        "MovieID", "Title", "AvgRating", "TotalRatings"
    ),
    "-" * 100
]

data = [
    "{:<7} | {:<60} | {:<10.2f} | {:<12}".format(
        x[0], x[1], x[2], x[3]
    )
    for x in all_movies
]

final_output = header + data

final_output += [
    "",
    "PHIM CÓ ĐIỂM CAO NHẤT (ÍT NHẤT 5 LƯỢT ĐÁNH GIÁ)",
    "=" * 100
]

if len(movies_5) > 0:
    best5 = max(movies_5, key=lambda x: x[2])

    final_output += header
    final_output.append(
        "{:<7} | {:<60} | {:<10.2f} | {:<12}".format(
            best5[0], best5[1], best5[2], best5[3]
        )
    )

final_output += [
    "",
    "PHIM CÓ ĐIỂM CAO NHẤT (ÍT NHẤT 50 LƯỢT ĐÁNH GIÁ)",
    "=" * 100
]

if len(movies_50) > 0:
    best50 = max(movies_50, key=lambda x: x[2])

    final_output += header
    final_output.append(
        "{:<7} | {:<60} | {:<10.2f} | {:<12}".format(
            best50[0], best50[1], best50[2], best50[3]
        )
    )
else:
    final_output += ["Không có phim nào có ít nhất 50 lượt đánh giá"] 

output = sc.parallelize(final_output)
output.coalesce(1).saveAsTextFile("/home/hadoop/lab3/output1")

sc.stop()
