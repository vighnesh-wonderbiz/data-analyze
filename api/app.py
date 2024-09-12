from fastapi import FastAPI
from typing import Optional
from datetime import datetime
from ..utils import Utils

app = FastAPI()

@app.get("/barChart")
async def BarChart(date: Optional[str] = None):
    if not date:
        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    l = Utils(date)
    res = l.generate_bar_data()
    return res
