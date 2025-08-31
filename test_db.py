import psycopg2

conn_str = "postgresql://postgres.gwzpsvjionjjcbhdnooe:9yPJ78CD7s2wceOr@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

try:
    conn = psycopg2.connect(conn_str)
    print("¡Conexión exitosa!")
    conn.close()
except Exception as e:
    print("Error de conexión:", e)