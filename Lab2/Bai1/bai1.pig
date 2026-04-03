-- 1. Load stopwords regex
regex_file = LOAD '/lab2/input/stopwords_regex.txt'
USING PigStorage() AS (pattern:chararray);

regex = LIMIT regex_file 1;

-- 2. Load CSV nguyên dòng (review có thể chứa ;)
raw_reviews = LOAD '/lab2/input/hotel-review.csv'
USING PigStorage('\n') AS (line:chararray);

-- 3. Bỏ header
filtered_reviews = FILTER raw_reviews BY NOT line MATCHES '^id;.*';

-- 4. Tách cột từ phải sang trái
split_reviews = FOREACH filtered_reviews GENERATE
    REGEX_EXTRACT(line, '^([^;]+);(.+);([^;]+);([^;]+);([^;]+)$', 1) AS id,
    REGEX_EXTRACT(line, '^([^;]+);(.+);([^;]+);([^;]+);([^;]+)$', 2) AS review,
    REGEX_EXTRACT(line, '^([^;]+);(.+);([^;]+);([^;]+);([^;]+)$', 3) AS aspect,
    REGEX_EXTRACT(line, '^([^;]+);(.+);([^;]+);([^;]+);([^;]+)$', 4) AS category,
    REGEX_EXTRACT(line, '^([^;]+);(.+);([^;]+);([^;]+);([^;]+)$', 5) AS sentiment;

-- 5. Chuyển review thành chữ thường
lower_reviews = FOREACH split_reviews GENERATE
    (int)id AS id,
    LOWER(review) AS review,
    aspect,
    category,
    sentiment;

-- 6. Loại stopwords
joined = CROSS lower_reviews, regex;

no_stopwords = FOREACH joined GENERATE
    id,
    REPLACE(review, pattern, '') AS review,
    aspect,
    category,
    sentiment;

-- 7. Loại tất cả dấu câu, thay bằng khoảng trắng nếu giữa từ
cleaned = FOREACH no_stopwords GENERATE
    id,
    TRIM(
        REPLACE(
            REPLACE(review, '[\\p{Punct}]', ' '),  -- tất cả dấu câu -> space
            '\\s{2,}', ' '                        -- nhiều space liên tiếp -> 1 space
        )
    ) AS review,
    aspect,
    category,
    sentiment;

-- 8. Sắp xếp và lưu kết quả
final = ORDER cleaned BY id, aspect, category;

STORE final INTO '/lab2/output1' USING PigStorage('\t');
