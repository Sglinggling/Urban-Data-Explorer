from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from endpoints import router as api_router

app = FastAPI(
    title="Urban Data Explorer API",
    version="1.0.0",
    description="API (zone GOLD) pour le projet Urban Data Explorer",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ok pour le dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "API Urban Data Explorer OK (GOLD)"}

app.include_router(api_router, prefix="/api")
