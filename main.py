import gc # 맨 위 import 영역에 추가

# --- 2. 심플해진 DB 저장 로직 (안전한 DB 연결 적용) ---
def update_database():
    print("🔄 거위 농장 ETF 데이터 수집 시작...")
    data = get_korean_etfs()
    
    db_url = os.environ.get("DB_URL") 
    if db_url:
        try:
            # with 구문을 쓰면 중간에 에러가 나도 자동으로 DB 연결을 닫아줍니다.
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS etf_farm_data (
                            id SERIAL PRIMARY KEY,
                            etf_441640_price FLOAT, etf_441640_div FLOAT, etf_441640_date VARCHAR(20),
                            etf_498400_price FLOAT, etf_498400_div FLOAT, etf_498400_date VARCHAR(20),
                            etf_491620_price FLOAT, etf_491620_div FLOAT, etf_491620_date VARCHAR(20),
                            etf_475720_price FLOAT, etf_475720_div FLOAT, etf_475720_date VARCHAR(20),
                            etf_0144l0_price FLOAT, etf_0144l0_div FLOAT, etf_0144l0_date VARCHAR(20),
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    cur.execute("""
                        INSERT INTO etf_farm_data (
                            id,
                            etf_441640_price, etf_441640_div, etf_441640_date,
                            etf_498400_price, etf_498400_div, etf_498400_date,
                            etf_491620_price, etf_491620_div, etf_491620_date,
                            etf_475720_price, etf_475720_div, etf_475720_date,
                            etf_0144l0_price, etf_0144l0_div, etf_0144l0_date
                        ) VALUES (
                            1,
                            %(etf_441640_price)s, %(etf_441640_div)s, %(etf_441640_date)s,
                            %(etf_498400_price)s, %(etf_498400_div)s, %(etf_498400_date)s,
                            %(etf_491620_price)s, %(etf_491620_div)s, %(etf_491620_date)s,
                            %(etf_475720_price)s, %(etf_475720_div)s, %(etf_475720_date)s,
                            %(etf_0144l0_price)s, %(etf_0144l0_div)s, %(etf_0144l0_date)s
                        ) ON CONFLICT (id) DO UPDATE SET 
                            etf_441640_price = EXCLUDED.etf_441640_price,
                            etf_441640_div = CASE WHEN EXCLUDED.etf_441640_div > 0 THEN EXCLUDED.etf_441640_div ELSE etf_farm_data.etf_441640_div END,
                            etf_441640_date = CASE WHEN EXCLUDED.etf_441640_date != 'N/A' THEN EXCLUDED.etf_441640_date ELSE etf_farm_data.etf_441640_date END,
                            etf_498400_price = EXCLUDED.etf_498400_price,
                            etf_498400_div = CASE WHEN EXCLUDED.etf_498400_div > 0 THEN EXCLUDED.etf_498400_div ELSE etf_farm_data.etf_498400_div END,
                            etf_498400_date = CASE WHEN EXCLUDED.etf_498400_date != 'N/A' THEN EXCLUDED.etf_498400_date ELSE etf_farm_data.etf_498400_date END,
                            etf_491620_price = EXCLUDED.etf_491620_price,
                            etf_491620_div = CASE WHEN EXCLUDED.etf_491620_div > 0 THEN EXCLUDED.etf_491620_div ELSE etf_farm_data.etf_491620_div END,
                            etf_491620_date = CASE WHEN EXCLUDED.etf_491620_date != 'N/A' THEN EXCLUDED.etf_491620_date ELSE etf_farm_data.etf_491620_date END,
                            etf_475720_price = EXCLUDED.etf_475720_price,
                            etf_475720_div = CASE WHEN EXCLUDED.etf_475720_div > 0 THEN EXCLUDED.etf_475720_div ELSE etf_farm_data.etf_475720_div END,
                            etf_475720_date = CASE WHEN EXCLUDED.etf_475720_date != 'N/A' THEN EXCLUDED.etf_475720_date ELSE etf_farm_data.etf_475720_date END,
                            etf_0144l0_price = EXCLUDED.etf_0144l0_price,
                            etf_0144l0_div = CASE WHEN EXCLUDED.etf_0144l0_div > 0 THEN EXCLUDED.etf_0144l0_div ELSE etf_farm_data.etf_0144l0_div END,
                            etf_0144l0_date = CASE WHEN EXCLUDED.etf_0144l0_date != 'N/A' THEN EXCLUDED.etf_0144l0_date ELSE etf_farm_data.etf_0144l0_date END,
                            updated_at = CURRENT_TIMESTAMP;
                    """, data)
            print("💾 거위 농장 DB 업데이트 완료!")
        except Exception as e:
            print("❌ DB 저장 실패:", e)

# --- 3. 백그라운드 무한 수집 봇 (청소 코드 추가) ---
def background_task():
    time.sleep(5) 
    while True:
        update_database()
        gc.collect() # 🧹 추가됨: 수집 후 메모리 찌꺼기 즉시 청소!
        time.sleep(3600)
