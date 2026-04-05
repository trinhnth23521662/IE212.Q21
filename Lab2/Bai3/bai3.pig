-- BÀI 3: Tìm aspect nhiều positive/negative nhất

-- LOAD dữ liệu
raw_data = LOAD '/lab2/output1/part-r-00000'
USING PigStorage('\t')
AS (
    id:int,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

-- Làm sạch dữ liệu
clean_data = FOREACH raw_data GENERATE
    TRIM(aspect) AS aspect,
    LOWER(TRIM(sentiment)) AS sentiment;

-- lọc null để tránh mất dữ liệu
clean_data = FILTER clean_data BY aspect IS NOT NULL AND sentiment IS NOT NULL;

-- NEGATIVE

negative_data = FILTER clean_data BY sentiment == 'negative';

grouped_neg = GROUP negative_data BY aspect;

count_neg = FOREACH grouped_neg GENERATE
    group AS aspect,
    COUNT(negative_data) AS total_negative;

grp_neg_all = GROUP count_neg ALL;

top_1_negative = FOREACH grp_neg_all {
    sorted = ORDER count_neg BY total_negative DESC;
    top1 = LIMIT sorted 1;
    GENERATE FLATTEN(top1);
};

-- POSITIVE

positive_data = FILTER clean_data BY sentiment == 'positive';

grouped_pos = GROUP positive_data BY aspect;

count_pos = FOREACH grouped_pos GENERATE
    group AS aspect,
    COUNT(positive_data) AS total_positive;

grp_pos_all = GROUP count_pos ALL;

top_1_positive = FOREACH grp_pos_all {
    sorted = ORDER count_pos BY total_positive DESC;
    top1 = LIMIT sorted 1;
    GENERATE FLATTEN(top1);
};

-- DUMP raw_data;
-- DUMP clean_data;
-- DUMP negative_data;
-- DUMP positive_data;

DUMP top_1_negative;
DUMP top_1_positive;

STORE top_1_negative INTO '/lab2/output3/negative'
USING PigStorage('\t');

STORE top_1_positive INTO '/lab2/output3/positive'
USING PigStorage('\t');
