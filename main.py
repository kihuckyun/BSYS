import yfinance as yf
import os
import time
import threading
import psycopg2
import gc  # 메모리 자동 청소기 도입
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

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

# --- 2. 심플해진 DB 저장 로직 (연결 누수 방지 적용) ---
def update_database():
    print("🔄 거위 농장 ETF 데이터 수집 시작...")
    data = get_korean_etfs()
    
    db_url = os.environ.get("DB_URL") 
    if db_url:
        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            
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
            conn.commit()
            print("💾 거위 농장 DB 업데이트 완료!")
        except Exception as e:
            print("❌ DB 저장 실패:", e)
            if conn: conn.rollback()
        finally:
            if conn:
                cur.close()
                conn.close()

# --- 3. 백그라운드 무한 수집 봇 (스마트 주기 적용) ---
def background_task():
    time.sleep(5) 
    
    # 한국 표준시(KST) 설정 (UTC+9)
    KST = timezone(timedelta(hours=9))
    
    while True:
        # DB 업데이트 및 메모리 청소 실행
        update_database()
        gc.collect() 
        
        # 현재 한국 시간 확인
        now_kst = datetime.now(KST)
        
        # 월~금 (0: 월요일, 4: 금요일)
        is_weekday = now_kst.weekday() < 5
        
        # 개장 시간 판별 (09:00 ~ 15:30)
        is_market_open = False
        if is_weekday:
            if now_kst.hour >= 9 and (now_kst.hour < 15 or (now_kst.hour == 15 and now_kst.minute <= 30)):
                is_market_open = True
        
        # 시간에 따른 대기(Sleep) 주기 분기
        if is_market_open:
            print(f"📈 [장중] 한국 시장이 열려있습니다. 1분(60초) 후 다시 수집합니다. (현재: {now_kst.strftime('%H:%M')})")
            time.sleep(60)
        else:
            print(f"💤 [장마감/휴일] 한국 시장이 닫혀있습니다. 1시간(3600초) 후 수집합니다. (현재: {now_kst.strftime('%H:%M')})")
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
    
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
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
    finally:
        if conn:
            cur.close()
            conn.close()

@app.post("/api/update_dividend")
def update_manual_dividend(data: ManualDivInput):
    db_url = os.environ.get("DB_URL")
    if not db_url: raise HTTPException(status_code=500, detail="DB Connection Error")
    
    target_ticker = data.ticker.lower()
    valid_tickers = ["441640", "498400", "491620", "475720", "0144l0"]
    
    if target_ticker not in valid_tickers:
        raise HTTPException(status_code=400, detail=f"Invalid Ticker: {data.ticker}")
        
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        div_col = f"etf_{target_ticker}_div"
        date_col = f"etf_{target_ticker}_date"
        
        query = f"""
            UPDATE etf_farm_data 
            SET {div_col} = %s, {date_col} = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1;
        """
        cur.execute(query, (data.dividend, data.ex_date))
        conn.commit()
                
        return {"status": "success", "message": f"{data.ticker} 배당금 수동 업데이트 완료"}
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
