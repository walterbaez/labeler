import httpx
from fastapi import HTTPException
import re
import os
import uuid
import psycopg
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
        print(f"Conexión exitosa!")
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        raise
    return conn

def get_or_create_assigned_to(request: Request, response: Response) -> str:
    assigned_to = request.cookies.get("assigned_to")
    if not assigned_to:
        assigned_to = str(uuid.uuid4())
        response.set_cookie(key="assigned_to", value=assigned_to, httponly=False, samesite="lax")
    return assigned_to

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
                print(f"Resultado de la consulta de imágenes: {row}")
                if not row:
                    conn.rollback()
                    return None
                img_id, url = row
                cur.execute(
                    "UPDATE images SET assigned_to=%s, assigned_at=%s WHERE id=%s AND assigned_at IS NULL",
                    [assigned_to, datetime.utcnow(), img_id],
                )
                print(f"Resultado de la actualización: {cur.rowcount} filas afectadas")
                if cur.rowcount == 1:
                    conn.commit()
                    return {"id": img_id, "url": url}
                conn.rollback()
        except Exception as e:
            print(f"Error en assign_one_random: {str(e)}")
            try:
                conn.rollback()
            except Exception:
                pass
            continue
    return None

# Create the `users` table if it doesn't exist
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
    """Validate that DATABASE_URL is set during application startup."""
    if DB_URL is None:
        raise Exception("DATABASE_URL no está configurada en las variables de entorno")
    print("DATABASE_URL validada correctamente.")

# Renombrar el helper y ajustar lógica
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    assigned_to = get_or_create_assigned_to(request, Response())
    conn = get_db()
    ensure_user_row(conn, assigned_to)
    user_data_complete = check_user_data_complete(conn, assigned_to)
    conn.close()

    next_path = "/task" if user_data_complete else "/intro"
    return templates.TemplateResponse("index.html", {"request": request, "next_path": next_path})

@app.get("/intro", response_class=HTMLResponse)
def intro_form(request: Request):
    assigned_to = get_or_create_assigned_to(request, Response())
    conn = get_db()
    ensure_user_row(conn, assigned_to)
    user_data_complete = check_user_data_complete(conn, assigned_to)
    conn.close()

    if user_data_complete:
        return RedirectResponse(url="/task", status_code=303)

    return templates.TemplateResponse("intro.html", {"request": request})

@app.post("/submit_intro")
def submit_intro(request: Request, response: Response, age_range: str = Form(...), meme_expertise: str = Form(...), political_position: str = Form(...)):
    assigned_to = get_or_create_assigned_to(request, response)
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (assigned_to, age_range, meme_expertise, political_position) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (assigned_to) DO UPDATE SET "
            "age_range = EXCLUDED.age_range, meme_expertise = EXCLUDED.meme_expertise, political_position = EXCLUDED.political_position",
            (assigned_to, age_range, meme_expertise, political_position)
        )
        conn.commit()
    conn.close()
    return RedirectResponse(url="/task", status_code=303)

# Adjust `/task` to assign an image immediately
@app.get("/task", response_class=HTMLResponse)
def task(request: Request):
    assigned_to = get_or_create_assigned_to(request, Response())
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
    has_hate: Optional[int] = Form(None),
):
    assigned_to = request.cookies.get("assigned_to")
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE images SET labeled=1, label_meme=%s, label_hate=%s, assigned_to=%s, submitted_at=%s WHERE id=%s",
            [is_meme, has_hate, assigned_to, datetime.utcnow(), image_id]
        )
        conn.commit()
    conn.close()
    return RedirectResponse(url="/task", status_code=303)

# Ensure `/done` simply shows the completion message
@app.get("/done", response_class=HTMLResponse)
def done(request: Request):
    return templates.TemplateResponse("done.html", {"request": request, "message": "No hay más imágenes disponibles para etiquetar."})

# 5. Export endpoints
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

# 6. Admin endpoints
@app.post("/admin/release_stale")
def release_stale():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE images SET assigned_at=NULL, assigned_to=NULL WHERE labeled=0 AND assigned_to IS NOT NULL"
        )
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
    # 1) Buscar la URL de esa imagen en la base
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT url FROM images WHERE id=%s", [image_id])
        row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Imagen no encontrada")
    url = row[0]

    try:
        # 2) Descargar la imagen desde la URL
        r = httpx.get(url, follow_redirects=True, timeout=30)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Error al cargar la imagen: {r.status_code}")
        ct = r.headers.get("content-type", "image/jpeg")
        return Response(content=r.content, media_type=ct)
    except httpx.RequestError as e:
        print(f"Error al cargar la imagen desde la URL: {e}")
        raise HTTPException(status_code=502, detail="Error al cargar la imagen")

# Helper functions

def ensure_user_row(conn, assigned_to):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users (assigned_to) VALUES (%s) ON CONFLICT DO NOTHING", (assigned_to,))
        conn.commit()

def check_user_data_complete(conn, assigned_to):
    with conn.cursor() as cur:
        cur.execute("SELECT age_range, meme_expertise, political_position FROM users WHERE assigned_to = %s", (assigned_to,))
        row = cur.fetchone()
        return row and all(row)
