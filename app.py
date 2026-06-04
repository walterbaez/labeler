import httpx
from fastapi import HTTPException

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import uuid
import psycopg
from datetime import datetime
from typing import Optional
import traceback


DB_URL = os.environ.get("DATABASE_URL")
ASSIGN_RETRIES = 5  # reintentos ante carrera
WORKERS_NOTE = "Con SQLite, corré con un solo proceso: uvicorn app:app --workers 1"

app = FastAPI(title="Image Labeler", version="1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def assign_one_random(conn, assigned_to: str):

    for attempt in range(1, ASSIGN_RETRIES + 1):
        try:
            with conn.cursor() as cur:
                # Buscar imagen disponible para slot 2 o 3
                # que el usuario no haya etiquetado antes
                cur.execute(
                    """
                    SELECT id FROM images
                    WHERE annotations_count < 3
                    AND annotator_2 != %s OR annotator_2 IS NULL
                    AND label_meme_1 IS NOT NULL
                    AND (
                        (annotations_count = 1 AND assigned_at_2 IS NULL)
                        OR
                        (annotations_count = 2 AND assigned_at_3 IS NULL)
                    )
                    AND id != ALL(
                        SELECT id FROM images WHERE annotator_2 = %s OR annotator_3 = %s
                    )
                    ORDER BY RANDOM()
                    LIMIT 1
                    """,
                    [assigned_to, assigned_to, assigned_to]
                )
                row = cur.fetchone()

                if not row:
                    conn.rollback()
                    return None

                img_id = row[0]

                # Determinar qué slot asignar
                cur.execute(
                    "SELECT annotations_count, assigned_at_2, assigned_at_3 FROM images WHERE id = %s",
                    [img_id]
                )
                img = cur.fetchone()
                count, at2, at3 = img

                if count == 1 and at2 is None:
                    cur.execute(
                        "UPDATE images SET annotator_2=%s, assigned_at_2=%s WHERE id=%s AND assigned_at_2 IS NULL RETURNING id",
                        [assigned_to, datetime.utcnow(), img_id]
                    )
                elif count == 2 and at3 is None:
                    cur.execute(
                        "UPDATE images SET annotator_3=%s, assigned_at_3=%s WHERE id=%s AND assigned_at_3 IS NULL RETURNING id",
                        [assigned_to, datetime.utcnow(), img_id]
                    )
                else:
                    conn.rollback()
                    continue

                updated = cur.fetchone()

                if updated:
                    conn.commit()
                    # Obtener URL para mostrar
                    cur.execute("SELECT url FROM images WHERE id=%s", [img_id])
                    url = cur.fetchone()[0]
                    return {"id": img_id, "url": url}
                conn.rollback()

        except Exception as e:
            print(f"[assign][ERROR] attempt {attempt}: {type(e).__name__}: {e}")
            traceback.print_exc()
            try:
                conn.rollback()
            except Exception:
                pass
            continue

    return None

def get_db():
    
    try:
        conn = psycopg.connect(
            DB_URL,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
       
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        raise
def ensure_user_exists(conn, assigned_to: Optional[str]):
    """
    Garantiza que exista una fila en users para el assigned_to dado.
    No crea cookies ni toca otras rutas; solo reconcilia cookie↔DB.
    """
    if not assigned_to:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (assigned_to) VALUES (%s) ON CONFLICT DO NOTHING",
            (assigned_to,)
        )

def check_user_data_complete(conn, assigned_to):
   
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (age_range IS NOT NULL)
              AND (meme_expertise IS NOT NULL)
              AND (political_position IS NOT NULL)
            FROM users
            WHERE assigned_to = %s
            """,
            (assigned_to,)
        )
        row = cur.fetchone()
        complete = bool(row and row[0])
       
        return complete
        
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
    has_cookie = bool(request.cookies.get("assigned_to"))
    need_seed = (request.url.path == "/" and not has_cookie)

    # opcional: solo para navegadores
    is_html_get = (
        request.method == "GET" and
        "text/html" in (request.headers.get("accept") or "")
    )

    response: Response = await call_next(request)

    if need_seed and is_html_get:
        new_val = f"user-{uuid.uuid4()}"
      
        response.set_cookie(
            key="assigned_to",
            value=new_val,
            max_age=365*24*60*60,
            httponly=True,
            samesite="lax",
            path="/",
        )
        
    return response
# =========================
# Rutas
# =========================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    
    conn = get_db()
    assigned_to = request.cookies.get("assigned_to")
    if assigned_to:
        user_data_complete = check_user_data_complete(conn, assigned_to)
        next_path = "/task" if user_data_complete else "/intro"
    else:
        next_path = "/intro"
    conn.close()
 
    return templates.TemplateResponse("index.html", {"request": request, "next_path": next_path})

@app.get("/intro", response_class=HTMLResponse)
def intro_form(request: Request):
    assigned_to = request.cookies.get("assigned_to")
    
    conn = get_db()

    #  NUEVO: reconciliar usuario si existe cookie pero falta fila en DB
    ensure_user_exists(conn, assigned_to)

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

    # NUEVO: reconciliar usuario si existe cookie pero falta fila en DB
    ensure_user_exists(conn, assigned_to)

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
    print("[/task] cookie:", assigned_to)
    conn = get_db()

    # 🔧 NUEVO: reconciliar usuario si existe cookie pero falta fila en DB
    ensure_user_exists(conn, assigned_to)

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
        # Determinar qué slot completar
        cur.execute(
            "SELECT annotator_2, annotator_3, annotations_count FROM images WHERE id=%s",
            [image_id]
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Imagen no encontrada")
        
        annotator_2, annotator_3, count = row

        if annotator_2 == assigned_to:
            cur.execute(
                """UPDATE images SET 
                    label_meme_2=%s, label_hate_2=%s, submitted_at_2=%s,
                    annotations_count = annotations_count + 1
                WHERE id=%s""",
                (is_meme, has_hate, datetime.utcnow(), image_id)
            )
        elif annotator_3 == assigned_to:
            cur.execute(
                """UPDATE images SET 
                    label_meme_3=%s, label_hate_3=%s, submitted_at_3=%s,
                    annotations_count = annotations_count + 1
                WHERE id=%s""",
                (is_meme, has_hate, datetime.utcnow(), image_id)
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
        # Liberar slot 2 asignado pero no completado
        cur.execute(
            """UPDATE images SET annotator_2=NULL, assigned_at_2=NULL
            WHERE assigned_at_2 IS NOT NULL AND submitted_at_2 IS NULL"""
        )
        released_2 = cur.rowcount

        # Liberar slot 3 asignado pero no completado
        cur.execute(
            """UPDATE images SET annotator_3=NULL, assigned_at_3=NULL
            WHERE assigned_at_3 IS NOT NULL AND submitted_at_3 IS NULL"""
        )
        released_3 = cur.rowcount

        conn.commit()
    conn.close()
    return {"released_slot_2": released_2, "released_slot_3": released_3}

@app.get("/admin", response_class=HTMLResponse)
def admin(request: Request):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM images")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM images WHERE annotations_count = 3")
        completas = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM images WHERE annotations_count = 2")
        dos_anotaciones = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM images WHERE annotations_count = 1")
        una_anotacion = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM images WHERE assigned_at_2 IS NOT NULL AND submitted_at_2 IS NULL")
        asignadas_2 = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM images WHERE assigned_at_3 IS NOT NULL AND submitted_at_3 IS NULL")
        asignadas_3 = cur.fetchone()[0]
    conn.close()
    html = f"""
        <html><head><link rel='stylesheet' href='/static/style.css'></head><body>
        <div class='container'>
            <h2>Progreso Inter-Anotador</h2>
            <div class='progress'>
                <table>
                    <tr><th>Total imágenes</th><td>{total}</td></tr>
                    <tr><th>3 anotaciones (completas)</th><td>{completas}</td></tr>
                    <tr><th>2 anotaciones</th><td>{dos_anotaciones}</td></tr>
                    <tr><th>1 anotación (original)</th><td>{una_anotacion}</td></tr>
                    <tr><th>Asignadas slot 2 sin completar</th><td>{asignadas_2}</td></tr>
                    <tr><th>Asignadas slot 3 sin completar</th><td>{asignadas_3}</td></tr>
                </table>
                <h3 style='margin-top:24px'>Liberar asignaciones sin completar</h3>
                <form onsubmit="event.preventDefault();
                    fetch('/admin/release_stale', {{method:'POST'}})
                        .then(r=>r.json())
                        .then(d=>{{ alert('Liberadas slot 2: ' + d.released_slot_2 + ' | slot 3: ' + d.released_slot_3); location.reload(); }})
                        .catch(()=>alert('Error liberando'));
                " style='margin-top:8px'>
                    <button class='btn' type='submit'>Liberar asignaciones</button>
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
