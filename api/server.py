from fastapi import FastAPI

from api.routes import router

app = FastAPI(title="Player Clustering API", version="0.1.0")
app.include_router(router)
