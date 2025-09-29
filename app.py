import httpx
from fastapi import HTTPException
from datetime import timedelta
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import re
import os
import uuid
import psycopg
from datetime import datetime
from typing import Optional

DB_URL = os.environ.get("DATABASE_URL")
ASSIGN_RETRIES = 5  # reintentos ante carrera
REQUIRE_TOKEN = os.environ.get("REQUIRE_TOKEN", "")  # si lo definís, exige ?token=...
WORKERS_NOTE = "Con SQLite, corré con un solo proceso: uvicorn app:app --workers 1"

app = FastAPI(title="Image Labeler", version="1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def assign_one_random(conn, assigned_to: str):
    """Assign one random image to the user."""
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
                    [assigned_to, datetime.utcnow(), img_id],
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

def get_db():
    print("Intentando conectar a la base de datos con URL:")
    try:
        conn = psycopg.connect(
            DB_URL,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        print("Conexión exitosa!")
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        raise

def check_user_data_complete(conn, assigned_to):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT age_range, meme_expertise, political_position FROM users WHERE assigned_to = %s",
            (assigned_to,)
        )
        row = cur.fetchone()
        return row and all(row)

@app.on_event("startup")
def create_users_table():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                assigned_to text UNIQUE NOT NULL,
                age_range INT,
                meme_expertise INT,
                political_position INT
            )
            """
        )
        conn.commit()
    conn.close()

@app.on_event("startup")
def validate_database_url():
    if DB_URL is None:
        raise Exception("DATABASE_URL no está configurada en las variables de entorno")
    print("DATABASE_URL validada correctamente.")

# =========================
# Middleware: crea cookie solo en "/"
# =========================
@app.middleware("http")
async def ensure_assigned_to_only_at_root(request: Request, call_next):
    need_seed = (
        request.url.path == "/"
        and not request.cookies.get("assigned_to")
    )
    new_val = f"user-{uuid.uuid4()}" if need_seed else None

    response: Response = await call_next(request)

    if need_seed:
        response.set_cookie(
            key="assigned_to",
            value=new_val,
            max_age=365*24*60*60,   # 1 año
            httponly=True,
            samesite="lax",
            path="/",
        )
        # Insert directo en DB
        try:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (assigned_to) VALUES (%s) ON CONFLICT DO NOTHING",
                    (new_val,)
                )
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error al asegurar usuario en users: {str(e)}")

    return response

# =========================
# Rutas
# =========================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    conn = get_db()
    assigned_to = request.cookies.get("assigned_to")  # puede ser None en la primera visita
    if assigned_to:
        user_data_complete = check_user_data_complete(conn, assigned_to)
        next_path = "/task" if user_data_complete else "/intro"
    else:
        # la cookie se setea al salir (middleware) en esta misma respuesta
        next_path = "/intro"
    conn.close()
    return templates.TemplateResponse("index.html", {"request": request, "next_path": next_path})

@app.get("/intro", response_class=HTMLResponse)
def intro_form(request: Request):
    assigned_to = request.cookies.get("assigned_to")
    conn = get_db()
    user_data_complete = check_user_data_complete(conn, assigned_to) if assigned_to else False
    conn.close()
    if user_data_complete:
        return RedirectResponse(url="/task", status_code=303)
    return templates.TemplateResponse("intro.html", {"request": request})

@app.post("/submit_intro")
def submit_intro(
    request: Request,
    age_range: int = Form(...),
    meme_expertise: int = Form(...),
    political_position: int = Form(...)
):
    assigned_to = request.cookies.get("assigned_to")
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET age_range = %s, meme_expertise = %s, political_position = %s WHERE assigned_to = %s",
            (age_range, meme_expertise, political_position, assigned_to)
        )
        conn.commit()
    conn.close()
    return RedirectResponse(url="/task", status_code=303)

@app.get("/task", response_class=HTMLResponse)
def task(request: Request):
    assigned_to = request.cookies.get("assigned_to")
    conn = get_db()
    data = assign_one_random(conn, assigned_to)
    conn.close()
    if not data:
        return RedirectResponse(url="/done", status_code=303)
    return templates.TemplateResponse("task.html", {"request": request, "id": data["id"], "url": data["url"]})

@app.post("/submit")
def submit(
    request: Request,
    image_id: str = Form(...),
    is_meme: int = Form(...),
    has_hate: Optional[int] = Form(None)
):
    assigned_to = request.cookies.get("assigned_to")
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE images SET labeled=1, label_meme=%s, label_hate=%s, assigned_to=%s, submitted_at=%s WHERE id=%s",
            (is_meme, has_hate, assigned_to, datetime.utcnow(), image_id)
        )
        conn.commit()
    conn.close()
    return RedirectResponse(url="/task", status_code=303)

@app.get("/done", response_class=HTMLResponse)
def done(request: Request):
    return templates.TemplateResponse("done.html", {"request": request, "message": "No hay más imágenes disponibles para etiquetar."})

@app.get("/export.csv")
def export_csv():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, url, labeled, label_meme, label_hate, assigned_to, assigned_at, submitted_at FROM images")
        rows = cur.fetchall()
    conn.close()
    header = "id,url,labeled,label_meme,label_hate,assigned_to,assigned_at,submitted_at\n"
    def gen():
        yield header
        for r in rows:
            yield ",".join("" if v is None else str(v) for v in r) + "\n"
    return StreamingResponse(gen(), media_type="text/csv")

@app.get("/export_labeled.csv")
def export_labeled_csv():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, url, labeled, label_meme, label_hate, assigned_to, assigned_at, submitted_at FROM images WHERE labeled=1")
        rows = cur.fetchall()
    conn.close()
    header = "id,url,labeled,label_meme,label_hate,assigned_to,assigned_at,submitted_at\n"
    def gen():
        yield header
        for r in rows:
            yield ",".join("" if v is None else str(v) for v in r) + "\n"
    return StreamingResponse(gen(), media_type="text/csv")

@app.post("/admin/release_stale")
def release_stale():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("UPDATE images SET assigned_at=NULL, assigned_to=NULL WHERE labeled=0 AND assigned_to IS NOT NULL")
        released = cur.rowcount
        conn.commit()
    conn.close()
    return {"released": released}

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
                    fetch('/admin/release_stale', {{method:'POST'}})
                        .then(r=>r.json())
                        .then(d=>{{ alert('Liberadas: ' + d.released); location.reload(); }})
                        .catch(()=>alert('Error liberando'));
                " style='margin-top:8px'>
                    <label>Re-liberar todas las imágenes asignadas y no etiquetadas</label>
                    <button class='btn' type='submit' style='margin-left:12px'>Liberar</button>
                </form>
            </div>
        </div>
        </body></html>
        """
    return HTMLResponse(html)

@app.get("/img/{image_id}")
def get_image(image_id: str):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT url FROM images WHERE id=%s", [image_id])
        row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Imagen no encontrada")
    url = row[0]
    try:
        r = httpx.get(url, follow_redirects=True, timeout=30)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Error al cargar la imagen: {r.status_code}")
        ct = r.headers.get("content-type", "image/jpeg")
        return Response(content=r.content, media_type=ct) 
    except httpx.RequestError as e:
        print(f"Error al cargar la imagen desde la URL: {e}") 
        raise HTTPException(status_code=502, detail="Error al cargar la imagen")
