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

def get_or_create_annotator_id(request: Request, response: Response) -> str:
    annotator_id = request.cookies.get("annotator_id")
    if not annotator_id:
        annotator_id = str(uuid.uuid4())
        response.set_cookie(key="annotator_id", value=annotator_id, httponly=False, samesite="lax")
    return annotator_id

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

# Simplify `/` endpoint to check user data and redirect accordingly
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    assigned_to = get_or_create_annotator_id(request, Response())
    conn = get_db()
    user_exists = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE assigned_to = %s", (assigned_to,))
            user_exists = cur.fetchone() is not None
            if not user_exists:
                cur.execute("INSERT INTO users (assigned_to) VALUES (%s)", (assigned_to,))
                conn.commit()
    except Exception as e:
        print(f"Error al verificar o crear el usuario: {str(e)}")
        return PlainTextResponse("Error interno", status_code=500)
    finally:
        conn.close()

    return templates.TemplateResponse("index.html", {"request": request, "user_exists": user_exists})

# Simplify `/intro` endpoint to only collect user data
@app.get("/intro", response_class=HTMLResponse)
def intro_form(request: Request):
    assigned_to = get_or_create_annotator_id(request, Response())
    conn = get_db()
    user_data = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT age_range, meme_expertise, political_position FROM users WHERE assigned_to = %s",
                (assigned_to,)
            )
            user_data = cur.fetchone()
    except Exception as e:
        print(f"Error al verificar datos del usuario: {str(e)}")
        return PlainTextResponse("Error interno", status_code=500)
    finally:
        conn.close()

    if user_data and all(user_data):
        return RedirectResponse(url="/task", status_code=303)

    return templates.TemplateResponse("intro.html", {"request": request})

@app.post("/submit_intro")
def submit_intro(request: Request, age_range: int = Form(...), meme_expertise: int = Form(...), political_position: int = Form(...)):
    assigned_to = get_or_create_annotator_id(request, Response())
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET age_range=%s, meme_expertise=%s, political_position=%s WHERE assigned_to=%s",
                (age_range, meme_expertise, political_position, assigned_to)
            )
            if cur.rowcount == 0:
                return PlainTextResponse("No se pudo actualizar los datos del usuario.", status_code=400)
            conn.commit()
    except Exception as e:
        print(f"Error al guardar datos del encuestado: {str(e)}")
        return PlainTextResponse("Error al guardar datos", status_code=500)
    finally:
        conn.close()
    return RedirectResponse(url="/task", status_code=303)

# Adjust `/task` to assign an image immediately
@app.get("/task", response_class=HTMLResponse)
def task(request: Request):
    assigned_to = get_or_create_annotator_id(request, Response())
    conn = get_db()
    data = assign_one_random(conn, assigned_to)
    conn.close()
    if not data:
        return RedirectResponse(url="/done", status_code=303)
    return templates.TemplateResponse("task.html", {"request": request, "id": data["id"], "url": data["url"]})

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
            """
            UPDATE images
               SET assigned_at = NULL,
                   assigned_to = NULL
             WHERE labeled = 0
               AND assigned_to IS NOT NULL
            """
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

@app.post("/submit")
def submit(
    request: Request,
    image_id: str = Form(...),
    is_meme: int = Form(...),
    has_hate: Optional[int] = Form(None),
):
    try:
        meme_val = int(is_meme)
        hate_val = int(has_hate)
    except (TypeError, ValueError):
        return PlainTextResponse("Las respuestas deben ser números entre 1 y 7", status_code=400)
    if not (1 <= meme_val <= 7):
        return PlainTextResponse("El puntaje de meme debe estar entre 1 y 7", status_code=400)
    if not (1 <= hate_val <= 7):
        return PlainTextResponse("El puntaje de odio debe estar entre 1 y 7", status_code=400)

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
            [meme_val, hate_val, annotator_id, datetime.utcnow(), image_id],
        )
        conn.commit()
    conn.close()
    return RedirectResponse(url="/task", status_code=303)
