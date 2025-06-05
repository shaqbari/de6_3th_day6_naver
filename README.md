# de6_3th_day6_naver
데이터엔지니어링 6기 세번째 프로젝트 6팀 DAY6

### .env파일 설정
```dotenv
NAVER_API_CLIENT_ID=your_id
NAVER_API_CLIENT_SECRET=your_secret
```

### docker 
```shell
docker compose up
```

## 사용할 postgres-naver db url
- 기본 설정 값
```python
params = {
    'host':"postgres-naver",     # ex) "localhost" 또는 "127.0.0.1"
    'port':"5432",               # 외부에서는 5433포트 사용
    'dbname':"naver",      # 접속할 데이터베이스 이름
    'user':"naver",      # 데이터베이스 사용자
    'password':"naver"   # 해당 사용자의 비밀번호
}
```

- airflow 컨테이너 내에서 사용
```python
from airflow.models import Variable


url = 'postgresql+psycopg2://naver:naver@postgres-naver/naver' 
# 또는  
url = Variable.get('POSTGRE_NAVER_CONN')
```

- 컨테이너 밖에서: 5433번 포트로 접속가능
```python
url = 'postgresql+psycopg2://naver:naver@localhost:5433/naver'
```
