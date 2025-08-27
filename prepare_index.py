import csv
import sqlite3
import argparse

SCHEMA = """
CREATE TABLE IF NOT EXISTS images (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    assigned_to TEXT,
    assigned_at TEXT,
    labeled INTEGER DEFAULT 0,
    label_meme INTEGER,
    label_hate INTEGER,
    annotator_id TEXT,
    submitted_at TEXT
);
"""

def upsert_image(conn, row):
    q = """
    INSERT INTO images (id, url) VALUES (?, ?)
    ON CONFLICT(id) DO UPDATE SET url=excluded.url;
    """
    conn.execute(q, (row["id"], row["url"]))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="CSV con columnas id,url")
    parser.add_argument("--db", default="images.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(SCHEMA)

    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            upsert_image(conn, row)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_images_labeled ON images(labeled);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_images_assigned ON images(assigned_at);")
    conn.close()
    print("OK: Base creada/actualizada", args.db)

if __name__ == "__main__":
    main()
