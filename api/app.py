from fastapi import FastAPI
from typing import Optional
from datetime import datetime
from utils import Utils
from urllib.parse import unquote

app = FastAPI()

@app.get("/bar-chart")
async def BarChart(date: Optional[str] = None):
    date = unquote(date)
    if not date:
        print(date)
        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    l = Utils(date)
    res = l.generate_bar_data()
    return res
