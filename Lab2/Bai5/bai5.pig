-- 1. LOAD dữ liệu
raw_data = LOAD '/lab2/output1/part-r-00000'
USING PigStorage('\t')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

-- 2. TOKENIZE (tách từ)
words = FOREACH raw_data GENERATE
    aspect,
    category,
    sentiment,
    FLATTEN(TOKENIZE(LOWER(review))) AS word;

-- 3. Loại ký tự đặc biệt
clean_words = FOREACH words GENERATE
    aspect,
    category,
    sentiment,
    REPLACE(word, '[^a-zA-Zà-ỹ0-9]', '') AS word;

-- 4. Loại từ rỗng
filtered_words = FILTER clean_words BY word IS NOT NULL AND word != '';

-- 5. Đếm tần suất từ
grouped = GROUP filtered_words BY (category, word);

word_count = FOREACH grouped GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(filtered_words) AS cnt;

-- 6. Group theo category
group_by_cat = GROUP word_count BY category;

-- 7. Sắp xếp và lấy top 5
top5 = FOREACH group_by_cat {
    sorted = ORDER word_count BY cnt DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
    };

-- 8. Lưu kết quả
STORE top5 INTO '/lab2/output5' USING PigStorage('\t');
