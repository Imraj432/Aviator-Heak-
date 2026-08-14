https://socket.io/import asyncio
from playwright.async_api import async_playwright
import json
from datetime import datetime
import redis.asyncio as redis
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class CrashScraper:
    def __init__(self, game_url: str):
        self.url = game_url
        self.redis = redis.from_url("redis://localhost:6379")
        self.influx = InfluxDBClient(url="http://localhost:8086", token="your-token", org="org")
        self.write_api = self.influx.write_api(write_options=SYNCHRONOUS)
        self.history = []  # last 200 multipliers

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context()
            page = await context.new_page()
            
            # Intercept WebSocket (most crash games use WS)
            await page.route("**/*", lambda route: route.continue_())
            
            page.on("websocket", self.handle_websocket)
            
            await page.goto(self.url, wait_until="networkidle")
            print("Connected to crash game...")
            await asyncio.sleep(3600 * 24)  # run forever

    async def handle_websocket(self, ws):
        print(f"WS connected: {ws.url}")
        ws.on("framesent", self.on_frame_sent)
        ws.on("framereceived", self.on_frame_received)

    def on_frame_received(self, frame):
        try:
            data = json.loads(frame.payload)
            if self.is_crash_update(data):
                multiplier = self.extract_multiplier(data)
                volume = self.extract_bet_volume(data)
                
                point = Point("crash_rounds") \
                    .tag("game", "target") \
                    .field("multiplier", multiplier) \
                    .field("bet_volume", volume or 0) \
                    .time(datetime.utcnow())
                
                self.write_api.write(bucket="crash", record=point)
                
                self.history.append(multiplier)
                if len(self.history) > 200:
                    self.history.pop(0)
                
                # Push to Redis for real-time consumers
                asyncio.create_task(self.redis.publish("crash:live", json.dumps({
                    "multiplier": multiplier,
                    "timestamp": datetime.utcnow().isoformat(),
                    "history": self.history[-50:]
                })))
        except:
            pass

    def is_crash_update(self, data: dict) -> bool:
        # Customize per game (look for "crash", "multiplier", "bust", etc.)
        return "multiplier" in str(data).lower() or "crash" in str(data).lower()

    def extract_multiplier(self, data: dict):
        # Implement game-specific parsing
        return float(data.get("multiplier", 1.0))import torch
import torch.nn as nn
import numpy as np
from xgboost import XGBRegressor
import joblib
from sklearn.preprocessing import MinMaxScaler

class CrashLSTM(nn.Module):
    def __init__(self, input_size=5, hidden_size=64, num_layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True, dropout=0.2)
        self.fc = nn.Linear(hidden_size, 3)  # Predict: prob_safe, expected_max, risk_score
    
    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        return self.fc(lstm_out[:, -1, :])

class CrashPredictor:
    def __init__(self):
        self.scaler = MinMaxScaler()
        self.lstm = CrashLSTM()
        self.xgb = XGBRegressor(n_estimators=200, learning_rate=0.05)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.lstm.to(self.device)
        
    def extract_features(self, history: list[float]) -> np.ndarray:
        """Short + long term features"""
        hist = np.array(history)
        features = []
        # Statistical
        features.extend([np.mean(hist[-10:]), np.std(hist[-10:]), 
                        (hist[-20:] < 1.5).mean(), (hist[-10:] > 10).sum()])
        # Streaks
        low_streak = sum(1 for x in reversed(hist) if x < 2.0)
        features.append(low_streak)
        # Rolling
        features.extend([np.percentile(hist[-50:], p) for p in [25, 50, 75]])
        return np.array(features).reshape(1, -1)

    def predict(self, recent_history: list[float], current_volume: float = 0):
        # LSTM sequence input
        seq = torch.tensor(self.scaler.fit_transform(np.array(recent_history[-50:]).reshape(-1,1)), 
                          dtype=torch.float32).unsqueeze(0).to(self.device)
        
        with torch.no_grad():
            lstm_pred = self.lstm(seq)
        
        xgb_features = self.extract_features(recent_history)
        xgb_pred = self.xgb.predict(xgb_features)
        
        # Ensemble
        safe_prob = float(torch.sigmoid(lstm_pred[0][0]).item() * 100)
        expected = float(lstm_pred[0][1].item() * 5 + xgb_pred[0])  # rough scaling
        
        risk = "Low" if safe_prob > 85 else "Medium" if safe_prob > 60 else "High"
        
        return {
            "safe_zone_prob": round(safe_prob, 1),
            "target_range": f"{max(1.0, expected-0.4):.2f}x - {expected+0.6:.2f}x",
            "risk_level": risk,
            "confidence": round(min(95, safe_prob * 0.7 + 30), 1)
        }from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from ingestion.scraper import CrashScraper
from ml.model import CrashPredictor

app = FastAPI()
predictor = CrashPredictor()
scraper = None

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_websockets=True)

@app.on_event("startup")
async def startup():
    global scraper
    scraper = CrashScraper("https://target-crash-site.com")
    asyncio.create_task(scraper.start())

@app.websocket("/ws/predictions")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    pubsub = scraper.redis.pubsub()
    await pubsub.subscribe("crash:live")
    
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message:
                data = json.loads(message["data"])
                pred = predictor.predict(data["history"], data.get("volume", 0))
                
                await websocket.send_json({
                    "timestamp": data["timestamp"],
                    "history": data["history"][-10:],
                    **pred
                })
            await asyncio.sleep(0.1)
    except:
        await pubsub.unsubscribe("crash:live")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)// Dashboard.tsx
import { useEffect, useState } from 'react';
import io from 'socket.io-client';

const socket = io("http://localhost:8000");

export default function CrashDashboard() {
  const [preds, setPreds] = useState(null);
  const [history, setHistory] = useState([]);

  useEffect(() => {
    socket.on("prediction", (data) => {
      setPreds(data);
      setHistory(data.history);
    });
  }, []);

  return (
    <div className="p-6 bg-zinc-950 text-white min-h-screen">
      <h1 className="text-4xl font-bold mb-8">Real-Time Crash AI Predictor</h1>
      
      {preds && (
        <div className="grid grid-cols-3 gap-6">
          <div className="bg-green-900 p-6 rounded-xl">
            <p className="text-sm opacity-70">Safe Zone (≥1.20x)</p>
            <p className="text-5xl font-mono">{preds.safe_zone_prob}%</p>
          </div>
          <div className="bg-blue-900 p-6 rounded-xl">
            <p className="text-sm opacity-70">Expected Range</p>
            <p className="text-5xl font-mono">{preds.target_range}</p>
          </div>
          <div className={`p-6 rounded-xl ${preds.risk_level === 'Low' ? 'bg-green-900' : 'bg-orange-900'}`}>
            <p className="text-sm opacity-70">Risk</p>
            <p className="text-5xl font-bold">{preds.risk_level}</p>
          </div>
        </div>
      )}

      {/* Live Chart with Recharts */}
      <HistoryChart data={history} />
    </div>
  );
      }
