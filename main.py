import yfinance as yf
import os
import time
import threading
import psycopg2
import gc  # 메모리 자동 청소기 도입
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# 👇 바로 이 부분이 누락되어서 발생한 에러입니다!
app = FastAPI()

# --- 보안 문 열어주기 (프론트엔드 연동용) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# [수동 데이터 입력 모델]
class ManualDivInput(BaseModel):
    ticker: str
    dividend: float
    ex_date: str

# --- 1. 가벼워진 ETF 전용 데이터 수집 함수 ---
def get_korean_etfs():
    """국내 ETF 5종의 현재가, 최근 분배금, 배당락일을 가져옵니다."""
    etf_list = ["441640", "498400", "491620", "475720", "0144L0"]
    etf_results = {}
    
    for code in etf_list:
        ticker = yf.Ticker(f"{code}.KS")
        db_code = code.lower()
        
        try:
            # 1년치 대신 3개월(3mo)로 최소화하여 속도 향상 및 에러 원천 차단
            hist = ticker.history(period="3mo")
            price = int(hist['Close'].iloc[-1]) if not hist.empty else 0
            
            if not hist.empty and 'Dividends' in hist.columns:
                divs = hist[hist['Dividends'] > 0]['Dividends']
                if not divs.empty:
                    ex_date = divs.index[-1].strftime('%Y-%m-%d')
                    div_amt = int(divs.iloc[-1])
                else:
                    ex_date = "N/A"
                    div_amt = 0
            else:
                ex_date = "N/A"
                div_amt = 0
                
        except Exception as e:
            print(f"[{code}] 데이터 수집 에러: {e}")
            price, ex_date, div_amt = 0, "N/A", 0
            
        etf_results[f"etf_{db_code}_price"] = price
        etf_results[f"etf_{db_code}_div"] = div_amt
        etf_results[f"etf_{db_code}_date"] = ex_date
        
    return etf_results

# --- 2. 심플해진 DB 저장 로직 (안전한 DB 연결 적용) ---
def update_database():
    print("🔄 거위 농장 ETF 데이터 수집 시작...")
    data = get_korean_etfs()
    
    db_url = os.environ.get("DB_URL") 
    if db_url:
        try:
            # with 구문: 중간에 에러가 나도 자동으로 DB 연결을 안전하게 닫아줍니다.
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    # 오직 ETF 5종만 담는 새 테이블 생성
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
                    
                    # 수동 입력 데이터 보호를 위한 조건부 업데이트 로직
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

# --- 3. 백그라운드 무한 수집 봇 ---
def background_task():
    time.sleep(5) 
    while True:
        update_database()
        gc.collect() # 🧹 수집 후 메모리 찌꺼기 즉시 청소!
        time.sleep(3600)

@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=background_task, daemon=True)
    t.start()

# --- 4. API 엔드포인트 ---
@app.get("/")
def read_root():
    return {"status": "Golden Goose ETF API is Running OK!"}

@app.get("/api/data")
def get_latest_data():
    db_url = os.environ.get("DB_URL")
    if not db_url: return {"error": "DB_URL is missing"}
    
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                # 5종 ETF 컬럼만 깔끔하게 불러옵니다.
                columns = [
                    "etf_441640_price", "etf_441640_div", "etf_441640_date",
                    "etf_498400_price", "etf_498400_div", "etf_498400_date",
                    "etf_491620_price", "etf_491620_div", "etf_491620_date",
                    "etf_475720_price", "etf_475720_div", "etf_475720_date",
                    "etf_0144l0_price", "etf_0144l0_div", "etf_0144l0_date",
                    "updated_at"
                ]
                
                query = f"SELECT {', '.join(columns)} FROM etf_farm_data WHERE id=1;"
                cur.execute(query)
                row = cur.fetchone()
                
        if row: return dict(zip(columns, row))
        else: return {"error": "No data yet. Waiting for first update..."}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/update_dividend")
def update_manual_dividend(data: ManualDivInput):
    """
    크롤링이 안 되는 종목의 배당금을 사용자가 웹앱에서 수동으로 업데이트하는 창구입니다.
    """
    db_url = os.environ.get("DB_URL")
    if not db_url: raise HTTPException(status_code=500, detail="DB Connection Error")
    
    target_ticker = data.ticker.lower()
    valid_tickers = ["441640", "498400", "491620", "475720", "0144l0"]
    
    if target_ticker not in valid_tickers:
        raise HTTPException(status_code=400, detail=f"Invalid Ticker: {data.ticker}")
        
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                div_col = f"etf_{target_ticker}_div"
                date_col = f"etf_{target_ticker}_date"
                
                query = f"""
                    UPDATE etf_farm_data 
                    SET {div_col} = %s, {date_col} = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1;
                """
                cur.execute(query, (data.dividend, data.ex_date))
                
        return {"status": "success", "message": f"{data.ticker} 배당금 수동 업데이트 완료"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
