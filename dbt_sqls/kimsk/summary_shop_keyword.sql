SELECT
    TO_CHAR(dt, 'YYYY-MM-DD HH24:MI:SS') AS dt, -- id는 시간 단위로 변환
    keyword,
    AVG(lprice)::bigint AS avg_price,
    MIN(lprice)::bigint AS min_price,
    MAX(lprice)::bigint AS max_price
FROM 
    {{ source('public', 'naver_shopping') }} -- dbt source 사용 (public 스키마의 naver_shopping 테이블)
WHERE 
    keyword IS NOT NULL
GROUP BY 
    TO_CHAR(dt, 'YYYY-MM-DD HH24:MI:SS'), -- 시간 단위로 그룹화
    keyword