from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import prices, news, macro, companies, causality, geo, summary, fundamentals

app = FastAPI(
    title="Monitorium API",
    description="Financial intelligence API for Kazakhstani markets",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(prices.router)
app.include_router(news.router)
app.include_router(macro.router)
app.include_router(companies.router)
app.include_router(causality.router)
app.include_router(geo.router)
app.include_router(summary.router)
app.include_router(fundamentals.router)


@app.get("/")
def health():
    return {"status": "ok"}
