SELECT 
    curr.id,
    curr.keyword,
    curr.avg_price,
    curr.min_price,
    curr.max_price,
    CASE 
        WHEN prev.avg_price IS NULL THEN 0
        WHEN ABS(curr.avg_price - prev.avg_price) / NULLIF(prev.avg_price, 0) >= 0.1 THEN 1
        ELSE 0
    END AS anomal
FROM {{ ref('summary_shop_keyword') }} curr -- dbt ref 함수 사용
LEFT JOIN {{ ref('summary_shop_keyword') }} prev -- dbt ref 함수 사용
    ON curr.keyword = prev.keyword
    AND prev.id = TO_CHAR(TO_TIMESTAMP(curr.id, 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '1 hour', 'YYYY-MM-DD HH24:MI:SS');