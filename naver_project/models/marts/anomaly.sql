SELECT 
    curr.id,
    curr.keyword,
    curr.avg_price,
    curr.min_price,
    curr.max_price,
    CASE 
        WHEN prev.avg_price IS NULL THEN 0
        WHEN diff_pct >= 0.05 THEN 1
        ELSE 0
    END AS anomal,
    -- 직전 시간 대비 가격 차이 비율
    CASE 
        WHEN prev.avg_price IS NULL THEN 0
        ELSE ABS(curr.avg_price - prev.avg_price) / NULLIF(prev.avg_price, 0)
    END AS diff_pct
FROM {{ ref('summary_shop_keyword') }} curr
LEFT JOIN {{ ref('summary_shop_keyword') }} prev
    ON curr.keyword = prev.keyword
    AND prev.id = TO_CHAR(
        TO_TIMESTAMP(curr.id, 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '1 hour',
        'YYYY-MM-DD HH24:MI:SS'
    )
