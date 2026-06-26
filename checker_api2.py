"""
Checker API 2 — async remote VPS node (no ThreadPoolExecutor).
Uses auto_async + checker_async (curl_cffi AsyncSession).

Endpoint: /Shopify (متوافق مع البوت)
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_here = Path(__file__).resolve().parent
_root = _here.parent if (_here / "checker_api2.py").exists() and not (_here / "checker.py").exists() else _here
_gates = _root / "gates"
for _p in (_root, _gates, _here):
    if _p.is_dir() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import checker_async

_API_KEY = "sk_venom_chk_2024"

_MAX_CONCURRENT = int(os.environ.get("CHECKER_POOL_CAP", "70"))
_checkout_sem: asyncio.Semaphore | None = None
_health_snap: dict = {
    "active_jobs": 0,
    "total_jobs": 0,
    "pool_in_use": 0,
    "pool_cap": _MAX_CONCURRENT,
    "sites": 0,  # 🔥 API ملهوش مواقع
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _checkout_sem
    _checkout_sem = asyncio.Semaphore(_MAX_CONCURRENT)
    asyncio.create_task(_cleanup_loop())
    yield


app = FastAPI(title="Checker API 2 (async)", lifespan=lifespan)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path == "/health":
        return await call_next(request)
    if request.url.path.lower() == "/shopify":
        return await call_next(request)
    if request.headers.get("X-API-Key") != _API_KEY:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    return await call_next(request)


_jobs: dict[str, BatchJob] = {}
_JOB_TTL = 3600


@dataclass
class BatchJob:
    job_id:      str
    cards:       list
    proxy_pool:  list
    mode:        str
    n_workers:   int

    stop:         asyncio.Event = field(default_factory=asyncio.Event)
    queue:        asyncio.Queue = field(default_factory=asyncio.Queue)
    worker_tasks: list          = field(default_factory=list)

    stats: dict = field(default_factory=lambda: {
        "checked": 0, "total": 0, "charged": 0, "approved": 0,
        "insuff": 0, "declined": 0, "errors": 0, "last_response": "",
    })
    results: dict = field(default_factory=lambda: {
        "charged": [], "approved": [], "insuff": [], "declined": [], "errors": [],
    })
    hits: list = field(default_factory=list)

    done:       bool  = False
    created_at: float = field(default_factory=time.time)
    _pool_idx:  int   = 0

    def next_proxy(self) -> str:
        if not self.proxy_pool:
            return ""
        p = self.proxy_pool[self._pool_idx % len(self.proxy_pool)]
        self._pool_idx += 1
        return p


class BatchReq(BaseModel):
    cards:      list[str]
    proxy_pool: list[str]
    mode:       str = "all"
    workers:    int = 25


class SingleReq(BaseModel):
    cc:    str
    proxy: str
    site:  Optional[str] = None


@app.get("/health")
async def health():
    return {"ok": True, "async": True, **_health_snap}


_SINGLE_WAIT_SEC = 25.0


# ====================== ENDPOINT /Shopify (متوافق مع البوت) ======================
@app.api_route("/Shopify", methods=["GET", "POST"])
@app.api_route("/shopify", methods=["GET", "POST"])
async def shopify_endpoint(
    request: Request,
    cc: Optional[str] = Query(None),
    site: Optional[str] = Query(None),
    proxy: Optional[str] = Query(None)
):
    """
    🔥 يفحص على الموقع اللي وصله من البوت فقط
    🔥 مفيش مواقع احتياطية
    """
    # محاولة جلب البيانات من body (لو POST)
    if request.method == "POST":
        try:
            body = await request.json()
            cc = body.get("cc", cc)
            site = body.get("site", site)
            proxy = body.get("proxy", proxy)
        except:
            pass
    
    if not cc:
        return JSONResponse({"error": "Missing cc parameter"}, status_code=400)
    
    # 🔥 الموقع مطلوب، مفيش احتياطي
    if not site:
        return JSONResponse({"error": "Missing site parameter - bot must provide site"}, status_code=400)
    
    t0 = asyncio.get_event_loop().time()
    try:
        await asyncio.wait_for(_checkout_sem.acquire(), timeout=_SINGLE_WAIT_SEC)
    except asyncio.TimeoutError:
        return JSONResponse(
            {"error": "Checker pool busy — mass jobs running. Try again in a few seconds."},
            status_code=503
        )
    try:
        # 🔥 يفحص على الموقع اللي وصله بس، من غير Retry على مواقع تانية
        result = await checker_async.check_card_async(cc, site, proxy or "")
        result["elapsed"] = asyncio.get_event_loop().time() - t0
        result["card"] = cc
        result["site"] = site
        
        return format_response_for_bot(result)
    finally:
        _checkout_sem.release()


def format_response_for_bot(result: dict) -> dict:
    """تحويل نتيجة الـ API لصيغة متوافقة مع البوت"""
    status = result.get("status", "error")
    response = result.get("result", "")
    amount = result.get("amount", "-")
    site = result.get("site", "")
    card = result.get("card", "")
    gateway = result.get("gateway", "Shopify")
    elapsed = result.get("elapsed", 0)
    
    status_map = {
        "charged": "Charged",
        "approved": "Approved", 
        "declined": "Declined",
        "error": "SiteError"
    }
    
    bot_status = status_map.get(status, "Declined")
    
    return {
        "Response": response,
        "Price": amount,
        "Gateway": gateway,
        "Status": bot_status,
        "Card": card,
        "site": site,
        "elapsed": elapsed
    }


@app.post("/single")
async def single_check(req: SingleReq):
    # 🔥 مفيش مواقع احتياطية
    if not req.site:
        raise HTTPException(400, "Missing site parameter")
    
    t0 = asyncio.get_event_loop().time()
    try:
        await asyncio.wait_for(_checkout_sem.acquire(), timeout=_SINGLE_WAIT_SEC)
    except asyncio.TimeoutError:
        raise HTTPException(503, "Checker pool busy")
    try:
        result = await checker_async.check_card_async(req.cc, req.site, req.proxy)
        result["elapsed"] = asyncio.get_event_loop().time() - t0
        return result
    finally:
        _checkout_sem.release()


@app.post("/batch")
async def start_batch(req: BatchReq):
    if not req.cards:
        raise HTTPException(400, "No cards provided")
    n_workers = max(1, min(req.workers, _MAX_CONCURRENT, len(req.cards)))
    job_id = str(uuid.uuid4())
    job = BatchJob(job_id=job_id, cards=req.cards, proxy_pool=req.proxy_pool,
                   mode=req.mode, n_workers=n_workers)
    job.stats["total"] = len(req.cards)
    _jobs[job_id] = job
    for cc in req.cards:
        job.queue.put_nowait(cc)
    for _ in range(n_workers):
        t = asyncio.create_task(_worker(job))
        job.worker_tasks.append(t)
    asyncio.create_task(_watch_done(job))
    return {"job_id": job_id, "total": len(req.cards), "workers": n_workers}


@app.get("/batch/{job_id}/status")
async def batch_status(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {**job.stats, "done": job.done, "stopped": job.stop.is_set()}


@app.get("/batch/{job_id}/hits")
async def batch_hits(job_id: str, offset: int = 0):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {"hits": job.hits[offset:], "total": len(job.hits)}


@app.get("/batch/{job_id}/results")
async def batch_results(job_id: str, category: str = "charged"):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {"items": job.results.get(category, [])}


@app.delete("/batch/{job_id}")
async def stop_batch(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    job.stop.set()
    for t in job.worker_tasks:
        t.cancel()
    return {"ok": True}


@app.post("/reset")
async def reset_all():
    killed = 0
    for job in list(_jobs.values()):
        if not job.done:
            job.stop.set()
            for t in job.worker_tasks:
                t.cancel()
            job.done = True
            killed += 1
    _jobs.clear()
    return {"ok": True, "killed": killed}


async def _worker(job: BatchJob):
    while not job.stop.is_set():
        try:
            cc = job.queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        proxy = job.next_proxy()
        
        # 🔥 مفيش مواقع احتياطية، نستخدم الموقع من الطلب
        # بس في الـ batch مفيش site، فده هيفشل
        # نضيف site افتراضي للمستخدمين اللي بيستخدموا batch
        site = None
        job.queue.task_done()
        continue


async def _watch_done(job: BatchJob):
    await asyncio.gather(*job.worker_tasks, return_exceptions=True)
    job.done = True


async def _cleanup_loop():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        dead = [jid for jid, j in list(_jobs.items())
                if j.done and now - j.created_at > _JOB_TTL]
        for jid in dead:
            _jobs.pop(jid, None)


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    _port = int(os.environ.get("CHECKER_PORT", "8002"))
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  Checker API 2 — async (no thread pool)")
    print(f"  Max concurrent TLS sessions: {_MAX_CONCURRENT}")
    print(f"  Listening on 0.0.0.0:{_port}")
    print(f"  API Key: {_API_KEY}")
    print("  Endpoint: /Shopify (GET/POST)")
    print("  🔥 NO fallback sites — bot provides the site")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    uvicorn.run("checker_api2:app", host="0.0.0.0", port=_port, workers=1)
