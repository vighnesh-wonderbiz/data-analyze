from fastapi import FastAPI
from typing import Optional
from datetime import datetime
from utils import Utils
from urllib.parse import unquote
import pandas as pd

app = FastAPI()

@app.get("/bar-chart")
async def BarChart(date: Optional[str] = None):
    if date:
        if date.startswith('"'):
            date = date.split('"')[1]
        date = unquote(date)
    if not date:
        print(date)
        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(date)
    l = Utils(date)
    res = l.generate_bar_data()
    return res
