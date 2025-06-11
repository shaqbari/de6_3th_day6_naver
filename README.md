# de6_3th_day6_naver
데이터엔지니어링 6기 세번째 프로젝트 6팀 DAY6

## .env파일 설정 예시
```dotenv
NAVER_API_CLIENT_ID=your_id
NAVER_API_CLIENT_SECRET=your_secret
DB_PORT=5432
DB_NAME=naver
DB_USER=naver
DB_PASSWORD=naver
NAVER_KEYWORD_JSON={"여름": ["제습기", "선풍기", "에어컨", "서큘레이터", "아이스박스"], "겨울": ["가습기", "히터", "보온병", "온풍기", "전기매트"]}
```

### airflow에서 .env파일에서 설정한 값 가져오기
```python
from airflow.models import Variable

client_id = Variable.get('NAVER_API_CLIENT_ID')
client_secret = Variable.get('NAVER_API_CLIENT_SECRET')
db_conn_url = Variable.get('POSTGRE_NAVER_CONN')

```


## docker 
```shell
docker compose up
```
