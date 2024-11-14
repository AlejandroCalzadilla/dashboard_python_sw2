from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from eureka_client import register_to_eureka
from database import transfer_data_to_postgres
import psycopg2
from datetime import datetime

# Inicia el microservicio FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir todos los orígenes
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos HTTP
    allow_headers=["*"],  # Permitir todas las cabeceras
)
# Registrar el servicio en Eureka
@app.on_event("startup")
async def startup_event():
    await register_to_eureka()
    transfer_data_to_postgres()

# Configurar la conexión a PostgreSQL
try:
    pg_conn = psycopg2.connect(
        dbname="postgres",
        user="postgres.biqvlcjahcefjcxxittl",
        password="ale12345678",
        host="aws-0-us-west-1.pooler.supabase.com",
        port="6543"
    )
    pg_cursor = pg_conn.cursor()
except psycopg2.OperationalError as e:
    print(f"Error al conectar a PostgreSQL: {e}")
    raise

# Endpoint de ejemplo
@app.get("/hello")
async def hello():
    return {"message": "Hello from sastreria-python"}

# Endpoint para transferir datos manualmente
@app.post("/transfer_data")
async def transfer_data():
    transfer_data_to_postgres()
    return {"message": "Data transferred successfully"}

# KPI 1: Ventas Totales por Mes
@app.get("/kpi/ventas_totales_por_mes")
async def ventas_totales_por_mes():
    query = """
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_price) AS total_sales
    FROM 
        orders
    WHERE 
        status = 'completado'
    GROUP BY 
        DATE_TRUNC('month', order_date)
    ORDER BY 
        month;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 2: Cantidad de Prendas Vendidas por Tipo
@app.get("/kpi/cantidad_prendas_por_tipo")
async def cantidad_prendas_por_tipo():
    query = """
    SELECT 
        g.name AS garment_type,
        SUM((oi->>'quantity')::INTEGER) AS total_quantity
    FROM 
        orders o,
        JSONB_ARRAY_ELEMENTS(order_items) oi
    JOIN 
        garments g ON g.id = (oi->>'garmentId')
    GROUP BY 
        g.name
    ORDER BY 
        total_quantity DESC
    LIMIT 5;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 3: Estados de Pedidos Actuales
@app.get("/kpi/estados_pedidos_actuales")
async def estados_pedidos_actuales():
    query = """
    SELECT 
        status, 
        COUNT(*) AS total_orders
    FROM 
        orders
    GROUP BY 
        status;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 4: Materiales más Utilizados
@app.get("/kpi/materiales_mas_utilizados")
async def materiales_mas_utilizados():
    query = """
    SELECT 
        rm.name AS material_name,
        SUM((dn->>'quantity')::INTEGER) AS total_used
    FROM 
        notes n,
        JSONB_ARRAY_ELEMENTS(detail_notes) dn
    JOIN 
        raw_materials rm ON rm.id = (dn->>'rawMaterialId')
    GROUP BY 
        rm.name
    ORDER BY 
        total_used DESC;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 5: Pedidos por Cliente
@app.get("/kpi/pedidos_por_cliente")
async def pedidos_por_cliente():
    query = """
    SELECT 
        c.first_name || ' ' || c.last_name AS customer_name,
        COUNT(o.id) AS total_orders,
        SUM(o.total_price) AS total_spent
    FROM 
        orders o
    JOIN 
        customers c ON o.customer_id = c.id
    GROUP BY 
        c.id, customer_name
    ORDER BY 
        total_spent DESC
    LIMIT 10;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 7: Cambios Realizados a Pedidos por Mes
@app.get("/kpi/cambios_realizados_por_mes")
async def cambios_realizados_por_mes():
    query = """
    SELECT 
        DATE_TRUNC('month', change_date) AS month,
        COUNT(*) AS total_changes
    FROM 
        order_changes
    GROUP BY 
        DATE_TRUNC('month', change_date)
    ORDER BY 
        month;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 8: Edad Promedio de los Clientes por Género
@app.get("/kpi/edad_promedio_por_genero")
async def edad_promedio_por_genero():
    query = """
    SELECT 
        sex,
        AVG(EXTRACT(YEAR FROM AGE(birth_date))) AS avg_age
    FROM 
        customers
    GROUP BY 
        sex;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 9: Costo Total por Prenda Producida
@app.get("/kpi/costo_total_por_prenda")
async def costo_total_por_prenda():
    query = """
    SELECT 
        g.name AS garment_name,
        SUM((dn->>'quantity')::INTEGER * (dn->>'price')::NUMERIC) AS total_cost
    FROM 
        notes n,
        JSONB_ARRAY_ELEMENTS(detail_notes) dn
    JOIN 
        garments g ON g.id = (dn->>'garmentId')
    GROUP BY 
        g.name
    ORDER BY 
        total_cost DESC;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 10: Promedio de Prendas por Pedido
@app.get("/kpi/promedio_prendas_por_pedido")
async def promedio_prendas_por_pedido():
    query = """
    SELECT 
        AVG(jsonb_array_length(order_items)) AS avg_items_per_order
    FROM 
        orders;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()
    return {"data": result[0]}

# KPI 11: Total de Ventas del Mes Actual
@app.get("/kpi/total_ventas_mes_actual")
async def total_ventas_mes_actual():
    query = """
    SELECT 
        SUM(total_price) AS total_sales
    FROM 
        orders
    WHERE 
        status = 'completado' AND
        DATE_TRUNC('month', order_date) = DATE_TRUNC('month', CURRENT_DATE);
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()
    return {"data": result[0]}

# KPI 12: Porcentaje de Pedidos Completados
@app.get("/kpi/porcentaje_pedidos_completados")
async def porcentaje_pedidos_completados():
    query = """
    SELECT 
        (COUNT(*) FILTER (WHERE status = 'completado') * 100.0 / COUNT(*)) AS porcentaje_completados
    FROM 
        orders;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()
    return {"data": result[0]}

# KPI 13: Promedio de Gasto por Cliente
@app.get("/kpi/promedio_gasto_por_cliente")
async def promedio_gasto_por_cliente():
    query = """
    SELECT 
        AVG(total_spent) AS avg_spent
    FROM (
        SELECT 
            SUM(total_price) AS total_spent
        FROM 
            orders
        GROUP BY 
            customer_id
    ) AS subquery;
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()
    return {"data": result[0]}

# Cerrar la conexión a PostgreSQL al finalizar
@app.on_event("shutdown")
async def shutdown_event():
    pg_cursor.close()
    pg_conn.close()