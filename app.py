"""One web process; actual consumers run as separate worker processes."""
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from shop import api

ROOT=Path(__file__).parent


@asynccontextmanager
async def lifespan(app):
    await api.startup()
    try:
        yield
    finally:
        await api.shutdown()


app=FastAPI(title='Mộc Studio · RabbitMQ Seminar',version='4.1',lifespan=lifespan)
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
app.include_router(api.router)


@app.get('/',include_in_schema=False)
def index():
    return FileResponse(ROOT/'static/index.html')


@app.get('/health',include_in_schema=False)
def health():
    return {'status':'ok'}
