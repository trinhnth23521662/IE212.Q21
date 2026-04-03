-- 1. LOAD DATA

bai1_data = LOAD '/lab2/output1/part-r-00000'
USING PigStorage('\t')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

-- 2. THỐNG KÊ TOP WORDS

words = FOREACH bai1_data 
    GENERATE FLATTEN(TOKENIZE(LOWER(review))) AS word;

words_valid = FILTER words BY word IS NOT NULL AND word != '';

grp_words = GROUP words_valid BY word;

word_count = FOREACH grp_words
GENERATE group AS word, COUNT(words_valid) AS freq;

all_data = GROUP word_count ALL;

top5_words = FOREACH all_data {
    sorted = ORDER word_count BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5);
};

-- 3. THỐNG KÊ CATEGORY

grouped_category = GROUP bai1_data BY category;

category_count = FOREACH grouped_category GENERATE
    group AS category,
    COUNT(bai1_data) AS total_comments;

category_count = ORDER category_count BY total_comments DESC;

-- 4. THỐNG KÊ ASPECT

grouped_aspect = GROUP bai1_data BY aspect;

aspect_count = FOREACH grouped_aspect GENERATE
    group AS aspect,
    COUNT(bai1_data) AS total_comments;

aspect_count = ORDER aspect_count BY total_comments DESC;

-- 5. STORE OUTPUT

STORE top5_words INTO '/lab2/output2/top5_words' USING PigStorage('\t');
STORE category_count INTO '/lab2/output2/category_count' USING PigStorage('\t');
STORE aspect_count INTO '/lab2/output2/aspect_count' USING PigStorage('\t');
