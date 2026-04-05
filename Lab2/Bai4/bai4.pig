raw_data = LOAD '/lab2/output1/part-r-00000'
USING PigStorage('\t')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

data = FILTER raw_data BY id != 'id';

data_clean = FOREACH data GENERATE
    review,
    category,
    LOWER(TRIM(sentiment)) AS sentiment;

pos = FILTER data_clean BY sentiment == 'positive';

words_pos = FOREACH pos GENERATE
    category,
    FLATTEN(TOKENIZE(review)) AS word;

words_pos_clean = FILTER words_pos BY
    word IS NOT NULL AND SIZE(word) > 2;

grp_pos = GROUP words_pos_clean BY (category, word);

count_pos = FOREACH grp_pos GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(words_pos_clean) AS freq;

grp_cat_pos = GROUP count_pos BY category;

top5_pos = FOREACH grp_cat_pos {
    sorted = ORDER count_pos BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};

neg = FILTER data_clean BY sentiment == 'negative';

words_neg = FOREACH neg GENERATE
    category,
    FLATTEN(TOKENIZE(review)) AS word;

words_neg_clean = FILTER words_neg BY
    word IS NOT NULL AND SIZE(word) > 2;

grp_neg = GROUP words_neg_clean BY (category, word);

count_neg = FOREACH grp_neg GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(words_neg_clean) AS freq;

grp_cat_neg = GROUP count_neg BY category;

top5_neg = FOREACH grp_cat_neg {
    sorted = ORDER count_neg BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};

STORE top5_pos INTO '/lab2/output4/top5_positive' USING PigStorage('\t');
STORE top5_neg INTO '/lab2/output4/top5_negative' USING PigStorage('\t');
