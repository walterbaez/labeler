
import httpx
from fastapi import HTTPException
import re
import os
import uuid
import psycopg2
from datetime import datetime
from typing import Optional
from datetime import timedelta
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

DB_URL = os.environ.get("DATABASE_URL")
ASSIGN_RETRIES = 5  # reintentos ante carrera
REQUIRE_TOKEN = os.environ.get("REQUIRE_TOKEN", "")  # si lo definís, exige ?token=...
WORKERS_NOTE = "Con SQLite, corré con un solo proceso: uvicorn app:app --workers 1"

app = FastAPI(title="Image Labeler", version="1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def get_db():
    conn = psycopg2.connect(DB_URL)
    return conn

def get_or_create_annotator_id(request: Request, response: Response) -> str:
    annotator_id = request.cookies.get("annotator_id")
    if not annotator_id:
        annotator_id = str(uuid.uuid4())
        response.set_cookie(key="annotator_id", value=annotator_id, httponly=False, samesite="lax")
    return annotator_id

@app.get("/", response_class=HTMLResponse)
def home(request: Request, token: Optional[str] = None):
    if REQUIRE_TOKEN and token != REQUIRE_TOKEN:
        return PlainTextResponse("Token inválido", status_code=401)
    return templates.TemplateResponse("index.html", {"request": request})

def assign_one_random(conn, annotator_id: str):
    # Asignación atómica: asegura que una imagen se entregue a una sola persona
    for _ in range(ASSIGN_RETRIES):
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN;")
                cur.execute(
                    "SELECT id, url FROM images WHERE assigned_at IS NULL AND labeled=0 ORDER BY RANDOM() LIMIT 1"
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    return None
                img_id, url = row
                cur.execute(
                    "UPDATE images SET assigned_to=%s, assigned_at=%s WHERE id=%s AND assigned_at IS NULL",
                    (annotator_id, datetime.utcnow().isoformat(), img_id),
                )
                if cur.rowcount == 1:
                    conn.commit()
                    return {"id": img_id, "url": url}
                conn.rollback()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            continue
    return None

@app.get("/task", response_class=HTMLResponse)
def task(request: Request, token: Optional[str] = None):
    if REQUIRE_TOKEN and token != REQUIRE_TOKEN:
        return PlainTextResponse("Token inválido", status_code=401)
    response = Response()
    annotator_id = get_or_create_annotator_id(request, response)
    conn = get_db()
    data = assign_one_random(conn, annotator_id)
    conn.close()
    if not data:
        html = templates.TemplateResponse("done.html", {"request": request})
        html.set_cookie(key="annotator_id", value=annotator_id, httponly=False, samesite="lax")
        return html
    content = templates.TemplateResponse("task.html", {"request": request, "id": data["id"], "url": data["url"]})
    content.set_cookie(key="annotator_id", value=annotator_id, httponly=False, samesite="lax")
    return content

@app.post("/submit")
def submit(
    request: Request,
    image_id: str = Form(...),
    is_meme: int = Form(...),
    has_hate: Optional[int] = Form(None),
):
    if int(is_meme) == 0:
        has_hate = 0
    elif has_hate is None:
        return PlainTextResponse("Falta campo 'has_hate'", status_code=400)

    annotator_id = request.cookies.get("annotator_id") or "unknown"
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE images
               SET labeled=1,
                   label_meme=%s,
                   label_hate=%s,
                   annotator_id=%s,
                   submitted_at=%s
             WHERE id=%s
            """,
            (int(is_meme), int(has_hate), annotator_id, datetime.utcnow().isoformat(), image_id),
        )
        conn.commit()
    conn.close()
    return RedirectResponse(url="/task", status_code=303)

@app.get("/export.csv")
def export_csv():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, url, labeled, label_meme, label_hate, annotator_id, assigned_to, assigned_at, submitted_at FROM images")
        rows = cur.fetchall()
    conn.close()
    header = "id,url,labeled,label_meme,label_hate,annotator_id,assigned_to,assigned_at,submitted_at\n"
    def gen():
        yield header
        for r in rows:
            yield ",".join("" if v is None else str(v) for v in r) + "\n"
    return StreamingResponse(gen(), media_type="text/csv")

@app.get("/export_labeled.csv")
def export_labeled_csv():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, url, labeled, label_meme, label_hate, annotator_id, assigned_to, assigned_at, submitted_at FROM images WHERE labeled=1")
        rows = cur.fetchall()
    conn.close()
    header = "id,url,labeled,label_meme,label_hate,annotator_id,assigned_to,assigned_at,submitted_at\n"
    def gen():
        yield header
        for r in rows:
            yield ",".join("" if v is None else str(v) for v in r) + "\n"
    return StreamingResponse(gen(), media_type="text/csv")

@app.get("/img/{image_id}")
def get_image(image_id: str):
    # 1) Buscar la URL de esa imagen en la base
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT url FROM images WHERE id=%s", (image_id,))
        row = cur.fetchone()
    conn.close()
    if not row:
        print(f"[PROXY] Imagen no encontrada para id: {image_id}")
        raise HTTPException(status_code=404, detail="Imagen no encontrada")
    url = row[0]
    print(f"[PROXY] Descargando imagen: {url} para id: {image_id}")
    try:
        r = httpx.get(url, follow_redirects=True, timeout=30)
        print(f"[PROXY] Status code: {r.status_code}")
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Upstream {r.status_code}")
        ct = r.headers.get("content-type", "image/jpeg")
        return Response(content=r.content, media_type=ct)
    except httpx.RequestError as e:
        print(f"[PROXY] Error: {e}")
        raise HTTPException(status_code=502, detail=str(e))
        fetch_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        # b) thumbnail grande (suele devolver image/jpeg, a veces más estable para <img>)
        # fetch_url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w1600"
    else:
        fetch_url = orig_url

    # 3) Elegir mime por extensión como fallback (por si upstream manda text/html)
    ext = image_id.lower().rsplit(".", 1)[-1] if "." in image_id else ""
    fallback_mime = {
        "jpg": "image/jpeg", "jpeg": "image/jpeg",
        "png": "image/png", "webp": "image/webp",
        "gif": "image/gif"
    }.get(ext, "application/octet-stream")

    # 4) Descargar y retransmitir
    try:
        with httpx.stream("GET", fetch_url, follow_redirects=True, timeout=30) as r:
            print(f"[PROXY] GET {fetch_url} -> {r.status_code} CT={r.headers.get('content-type')}")
            if r.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Upstream {r.status_code}")
            ct = r.headers.get("content-type") or fallback_mime
            # Si por algún motivo vino text/html, forzamos el fallback
            if ct.startswith("text/html"):
                ct = fallback_mime
            return StreamingResponse(r.iter_bytes(), media_type=ct, headers={
                "Cache-Control": "public, max-age=3600"
            })
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.get("/done", response_class=HTMLResponse)
def done(request: Request):
    return templates.TemplateResponse("done.html", {"request": request})


# endpoint: liberar asignaciones viejas (DEFAULT = 20 minutos)
@app.post("/admin/release_stale")
def release_stale(minutes: int = 20):
    cutoff = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE images
               SET assigned_at = NULL,
                   assigned_to = NULL
             WHERE labeled = 0
               AND assigned_at IS NOT NULL
               AND assigned_at < %s
            """,
            (cutoff,)
        )
        released = cur.rowcount
        conn.commit()
    conn.close()
    return {"released": released, "cutoff": cutoff, "minutes": minutes}


@app.get("/admin", response_class=HTMLResponse)
def admin(request: Request):
        conn = get_db()
        with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM images")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM images WHERE labeled=1")
                labeled = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM images WHERE assigned_at IS NOT NULL")
                assigned = cur.fetchone()[0]
        conn.close()
        html = f"""
        <html><head><link rel='stylesheet' href='/static/style.css'></head><body>
        <div class='container'>
            <h2>Progreso</h2>
            <div class='progress'>
                <table>
                    <tr><th>Total</th><td>{total}</td></tr>
                    <tr><th>Etiquetadas</th><td>{labeled}</td></tr>
                    <tr><th>Asignadas (en curso)</th><td>{assigned}</td></tr>
                    <tr><th>No asignadas</th><td>{total - assigned}</td></tr>
                </table>
                <p>{WORKERS_NOTE}</p>
                <h3 style='margin-top:24px'>Liberar asignaciones viejas</h3>
                <form onsubmit="event.preventDefault();
                    const mins = document.getElementById('mins').value || 20;
                    fetch('/admin/release_stale?minutes=' + mins, {{method:'POST'}})
                        .then(r=>r.json())
                        .then(d=>{{ alert('Liberadas: ' + d.released + '\nCorte: ' + d.cutoff); location.reload(); }})
                        .catch(()=>alert('Error liberando'));
                " style='margin-top:8px'>
                    <label>Re-liberar imágenes no etiquetadas asignadas hace &gt; 
                        <input id='mins' type='number' value='20' min='1' style='width:80px'> min
                    </label>
                    <button class='btn' type='submit' style='margin-left:12px'>Liberar</button>
                </form>
            </div>
        </div>
        </body></html>
        """
        return HTMLResponse(html)
