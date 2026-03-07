import yfinance as yf
import pandas as pd
import requests
import psycopg2
import os
import time
import threading
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# --- 보안 문 열어주기 (웹앱이 접속할 수 있게 허용) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# [수동 데이터 입력을 위한 데이터 모델 (Pydantic)]
class ManualDivInput(BaseModel):
    ticker: str         # 예: "475720"
    dividend: float     # 예: 100
    ex_date: str        # 예: "2026-03-31"

# --- 1. 기존 데이터 수집 함수들 ---
def get_price(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return round(float(hist['Close'].iloc[-1]), 2)
        
        hist_d = ticker.history(period="1d")
        return round(float(hist_d['Close'].iloc[-1]), 2) if not hist_d.empty else 0
    except Exception as e:
        print(f"{ticker_symbol} 가격 수집 오류: {e}")
        return 0

def get_rsi(ticker_symbol="QQQ", period=14):
    try:
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="1y")
        if df.empty: return 50

        delta = df['Close'].diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)

        ema_up = up.ewm(com=period - 1, adjust=False).mean()
        ema_down = down.ewm(com=period - 1, adjust=False).mean()

        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))
        return round(float(rsi.iloc[-1]), 2)
    except Exception as e:
        print(f"RSI 수집 오류: {e}")
        return 50

def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        score = data['fear_and_greed']['score']
        return round(float(score), 2)
    except Exception as e:
        print(f"피어앤그리드 수집 오류: {e}")
        return 50 

# --- 2. 신규 추가: 국내 배당 ETF 수집 함수 ---
def get_korean_etfs():
    etf_list = ["441640", "498400", "491620", "475720", "0144L0"]
    etf_results = {}
    
    for code in etf_list:
        ticker = yf.Ticker(f"{code}.KS")
        db_code = code.lower()
        
        try:
            hist = ticker.history(period="1y")
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
        # 크롤링 실패시 0이 들어가므로, 나중에 수동입력 API를 통해 덮어쓸 수 있습니다.
        etf_results[f"etf_{db_code}_div"] = div_amt
        etf_results[f"etf_{db_code}_date"] = ex_date
        
    return etf_results

# --- 3. DB 저장 로직 (업그레이드 완료) ---
def update_database():
    print("🔄 자동 데이터 수집을 시작합니다...")
    data = {
        'qqqm': get_price("QQQM"), 'qld': get_price("QLD"), 'sgov': get_price("SGOV"),
        'iau': get_price("IAU"), 'vix': get_price("^VIX"), 'fx': get_price("KRW=X"),
        'rsi': get_rsi("QQQ"), 'fg': get_fear_and_greed()
    }
    
    etf_data = get_korean_etfs()
    data.update(etf_data) 
    
    db_url = os.environ.get("DB_URL") 
    if db_url:
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS market_data_v2 (
                    id SERIAL PRIMARY KEY,
                    qqqm_price FLOAT, qld_price FLOAT, sgov_price FLOAT, iau_price FLOAT,
                    vix FLOAT, fx FLOAT, rsi FLOAT, fg FLOAT,
                    etf_441640_price FLOAT, etf_441640_div FLOAT, etf_441640_date VARCHAR(20),
                    etf_498400_price FLOAT, etf_498400_div FLOAT, etf_498400_date VARCHAR(20),
                    etf_491620_price FLOAT, etf_491620_div FLOAT, etf_491620_date VARCHAR(20),
                    etf_475720_price FLOAT, etf_475720_div FLOAT, etf_475720_date VARCHAR(20),
                    etf_0144l0_price FLOAT, etf_0144l0_div FLOAT, etf_0144l0_date VARCHAR(20),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 주의: 수동 업데이트를 존중하기 위해, 가격 정보만 항상 덮어쓰고
            # 배당금 정보는 야후 파이낸스에서 0보다 큰 정상 값을 긁어왔을 때만 덮어씁니다.
            # (만약 475720이 0으로 긁혀왔다면, 사용자가 수동입력한 값을 유지함)
            cur.execute("""
                INSERT INTO market_data_v2 (
                    id, qqqm_price, qld_price, sgov_price, iau_price, vix, fx, rsi, fg,
                    etf_441640_price, etf_441640_div, etf_441640_date,
                    etf_498400_price, etf_498400_div, etf_498400_date,
                    etf_491620_price, etf_491620_div, etf_491620_date,
                    etf_475720_price, etf_475720_div, etf_475720_date,
                    etf_0144l0_price, etf_0144l0_div, etf_0144l0_date
                ) VALUES (
                    1, %(qqqm)s, %(qld)s, %(sgov)s, %(iau)s, %(vix)s, %(fx)s, %(rsi)s, %(fg)s,
                    %(etf_441640_price)s, %(etf_441640_div)s, %(etf_441640_date)s,
                    %(etf_498400_price)s, %(etf_498400_div)s, %(etf_498400_date)s,
                    %(etf_491620_price)s, %(etf_491620_div)s, %(etf_491620_date)s,
                    %(etf_475720_price)s, %(etf_475720_div)s, %(etf_475720_date)s,
                    %(etf_0144l0_price)s, %(etf_0144l0_div)s, %(etf_0144l0_date)s
                ) ON CONFLICT (id) DO UPDATE SET 
                    qqqm_price = EXCLUDED.qqqm_price, qld_price = EXCLUDED.qld_price,
                    sgov_price = EXCLUDED.sgov_price, iau_price = EXCLUDED.iau_price,
                    vix = EXCLUDED.vix, fx = EXCLUDED.fx, rsi = EXCLUDED.rsi, fg = EXCLUDED.fg,
                    
                    etf_441640_price = EXCLUDED.etf_441640_price,
                    etf_441640_div = CASE WHEN EXCLUDED.etf_441640_div > 0 THEN EXCLUDED.etf_441640_div ELSE market_data_v2.etf_441640_div END,
                    etf_441640_date = CASE WHEN EXCLUDED.etf_441640_date != 'N/A' THEN EXCLUDED.etf_441640_date ELSE market_data_v2.etf_441640_date END,
                    
                    etf_498400_price = EXCLUDED.etf_498400_price,
                    etf_498400_div = CASE WHEN EXCLUDED.etf_498400_div > 0 THEN EXCLUDED.etf_498400_div ELSE market_data_v2.etf_498400_div END,
                    etf_498400_date = CASE WHEN EXCLUDED.etf_498400_date != 'N/A' THEN EXCLUDED.etf_498400_date ELSE market_data_v2.etf_498400_date END,
                    
                    etf_491620_price = EXCLUDED.etf_491620_price,
                    etf_491620_div = CASE WHEN EXCLUDED.etf_491620_div > 0 THEN EXCLUDED.etf_491620_div ELSE market_data_v2.etf_491620_div END,
                    etf_491620_date = CASE WHEN EXCLUDED.etf_491620_date != 'N/A' THEN EXCLUDED.etf_491620_date ELSE market_data_v2.etf_491620_date END,
                    
                    etf_475720_price = EXCLUDED.etf_475720_price,
                    etf_475720_div = CASE WHEN EXCLUDED.etf_475720_div > 0 THEN EXCLUDED.etf_475720_div ELSE market_data_v2.etf_475720_div END,
                    etf_475720_date = CASE WHEN EXCLUDED.etf_475720_date != 'N/A' THEN EXCLUDED.etf_475720_date ELSE market_data_v2.etf_475720_date END,
                    
                    etf_0144l0_price = EXCLUDED.etf_0144l0_price,
                    etf_0144l0_div = CASE WHEN EXCLUDED.etf_0144l0_div > 0 THEN EXCLUDED.etf_0144l0_div ELSE market_data_v2.etf_0144l0_div END,
                    etf_0144l0_date = CASE WHEN EXCLUDED.etf_0144l0_date != 'N/A' THEN EXCLUDED.etf_0144l0_date ELSE market_data_v2.etf_0144l0_date END,
                    
                    updated_at = CURRENT_TIMESTAMP;
            """, data)
            conn.commit()
            cur.close()
            conn.close()
            print("💾 DB 저장 완료!")
        except Exception as e:
            print("❌ DB 저장 실패:", e)

# --- 4. 무한 반복 로봇 세팅 ---
def background_task():
    time.sleep(5) 
    while True:
        update_database()
        time.sleep(3600)

@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=background_task, daemon=True)
    t.start()

# --- 5. 웹 API 창구 ---
@app.get("/")
def read_root():
    return {"status": "Q7S3 Bot + ETF Tracker is Running OK!"}

@app.get("/api/data")
def get_latest_data():
    db_url = os.environ.get("DB_URL")
    if not db_url: return {"error": "DB_URL is missing"}
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        columns = [
            "qqqm_price", "qld_price", "sgov_price", "iau_price", "vix", "fx", "rsi", "fg",
            "etf_441640_price", "etf_441640_div", "etf_441640_date",
            "etf_498400_price", "etf_498400_div", "etf_498400_date",
            "etf_491620_price", "etf_491620_div", "etf_491620_date",
            "etf_475720_price", "etf_475720_div", "etf_475720_date",
            "etf_0144l0_price", "etf_0144l0_div", "etf_0144l0_date",
            "updated_at"
        ]
        query = f"SELECT {', '.join(columns)} FROM market_data_v2 WHERE id=1;"
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row: return dict(zip(columns, row))
        else: return {"error": "No data yet. Waiting for first update..."}
    except Exception as e:
        return {"error": str(e)}

# ⚡ [신규] 프론트엔드에서 수동으로 배당금을 덮어쓰는 POST API
@app.post("/api/update_dividend")
def update_manual_dividend(data: ManualDivInput):
    """
    야후에서 못 긁어오는 475720 같은 종목의 배당금을 수동으로 DB에 업데이트합니다.
    """
    db_url = os.environ.get("DB_URL")
    if not db_url: raise HTTPException(status_code=500, detail="DB Connection Error")
    
    # 전달받은 티커를 소문자로 변경
    target_ticker = data.ticker.lower()
    valid_tickers = ["441640", "498400", "491620", "475720", "0144l0"]
    
    if target_ticker not in valid_tickers:
        raise HTTPException(status_code=400, detail=f"Invalid Ticker: {data.ticker}")
        
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # SQL Injection을 막기 위해 f-string으로 컬럼명만 동적으로 생성
        div_col = f"etf_{target_ticker}_div"
        date_col = f"etf_{target_ticker}_date"
        
        query = f"""
            UPDATE market_data_v2 
            SET {div_col} = %s, {date_col} = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1;
        """
        cur.execute(query, (data.dividend, data.ex_date))
        conn.commit()
        
        cur.close()
        conn.close()
        return {"status": "success", "message": f"{data.ticker} 배당금 수동 업데이트 완료", "new_dividend": data.dividend}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)