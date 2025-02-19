from fastapi import FastAPI
from app.routers import insphire_moe

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Please Navigate to Swagger Docs to see end points. Hit /docs with local url"}

# Include the router
app.include_router(insphire_moe.router)
