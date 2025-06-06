
# de6_3th_day6_naver
데이터엔지니어링 6기 세번째 프로젝트 6팀 DAY6

## 🔧 .env 파일 설정
```
NAVER_API_CLIENT_ID=GYvIPmBAxmZBaop4vaMI
NAVER_API_CLIENT_SECRET=q1n4sIXwqg
```
- 코드에서는 .env가 아닌 Web UI의 variable을 호출하고 있어 생략 가능

## 📌 Airflow Web UI 설정 방법

### 🔹 Variables 설정
Web UI → Admin → Variables → 다음 항목들을 등록합니다:

| Key                  | Value (예시)                                                                                                                                       |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `NAVER_CLIENT_ID`     | `본인의 id`                                                                                                                              |
| `NAVER_CLIENT_SECRET` | `본인의 secret`                                                                                                                                        |
| `NAVER_KEYWORD_MAP`   | 아래 JSON 형식                                                                                                                                       |

```json
{
  "제습기": "여름",
  "선풍기": "여름",
  "에어컨": "여름",
  "서큘레이터": "여름",
  "아이스박스": "여름",
  "가습기": "겨울",
  "히터": "겨울",
  "보온병": "겨울",
  "온풍기": "겨울",
  "전기매트": "겨울"
}
```

### 🔹 Connections 설정
Web UI → Admin → Connections → 새 Connection 생성

| 항목                  | 값                                       |
|----------------------|------------------------------------------|
| **Connection Id**    | `my_postgres_conn_id` |
| **Connection Type**  | `Postgres`            |
| **Host**             | `postgres`            |
| **Database**         | `naver`               |
| **Login**            | `naver`               |
| **Password**         | `naver`               |
| **Port**             | `5433`                |

---

## 🐳 Docker 실행
```bash
docker compose up
```

---

## 🛠️ 사용할 Postgres-Naver DB URL

### Airflow 컨테이너 내에서
```python
url = 'postgresql+psycopg2://naver:naver@postgres-naver/naver'
# 또는 Variable 사용 시
from airflow.models import Variable
url = Variable.get('POSTGRE_NAVER_CONN')
```

### 외부에서 (로컬 개발환경)
```python
url = 'postgresql+psycopg2://naver:naver@localhost:5433/naver'
```

---

## 📌 Airflow DAG에서 환경변수 사용 예시

```python
from airflow.models import Variable

client_id = Variable.get("NAVER_CLIENT_ID")
client_secret = Variable.get("NAVER_CLIENT_SECRET")
```
