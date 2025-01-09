from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import transfer_data_to_postgres
from db import get_db_connection
import psycopg2
from datetime import datetime
from querys import *
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
# Inicia el microservicio FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir todos los orígenes
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos HTTP
    allow_headers=["*"],  # Permitir todas las cabeceras
)
pg_conn, pg_cursor = get_db_connection()


def update_db_connection():
    global pg_conn, pg_cursor
    pg_conn, pg_cursor = get_db_connection()
    print("Conexión a la base de datos actualizada")

# Configurar el programador
scheduler = AsyncIOScheduler()
scheduler.add_job(update_db_connection, CronTrigger(hour=2, minute=0))  # Ejecutar todos los días a las 2:00 AM
scheduler.start()





# Registrar el servicio en Eureka
@app.on_event("startup")
async def startup_event():
    transfer_data_to_postgres(pg_conn, pg_cursor)






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
    pg_cursor.execute(ventas_totales_por_mes_query)
    result = pg_cursor.fetchall()
    return {"data": result}




# KPI 2: Cantidad de Prendas Vendidas por Tipo
@app.get("/kpi/cantidad_prendas_por_tipo")
async def cantidad_prendas_por_tipo():
    pg_cursor.execute(cantidad_prendas_por_tipo_query)
    result = pg_cursor.fetchall()
    return {"data": result}





# KPI 3: Estados de Pedidos Actuales
@app.get("/kpi/estados_pedidos_actuales")
async def estados_pedidos_actuales():
    pg_cursor.execute(estados_pedidos_actuales_query)
    result = pg_cursor.fetchall()
    return {"data": result}






# KPI 4: Materiales más Utilizados
@app.get("/kpi/materiales_mas_utilizados")
async def materiales_mas_utilizados():
    pg_cursor.execute(materiales_mas_utilizados_query)
    result = pg_cursor.fetchall()
    return {"data": result}




# KPI 5: Pedidos por Cliente
@app.get("/kpi/pedidos_por_cliente")
async def pedidos_por_cliente():
    pg_cursor.execute(pedidos_por_cliente_query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 7: Cambios Realizados a Pedidos por Mes
@app.get("/kpi/cambios_realizados_por_mes")
async def cambios_realizados_por_mes():
    pg_cursor.execute(cambios_realizados_por_mes_query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 8: Edad Promedio de los Clientes por Género
@app.get("/kpi/edad_promedio_por_genero")
async def edad_promedio_por_genero():
    pg_cursor.execute(edad_promedio_por_genero_query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 9: Costo Total por Prenda Producida
@app.get("/kpi/costo_total_por_prenda")
async def costo_total_por_prenda():
    pg_cursor.execute(costo_total_por_prenda_query)
    result = pg_cursor.fetchall()
    return {"data": result}

# KPI 10: Promedio de Prendas por Pedido
@app.get("/kpi/promedio_prendas_por_pedido")
async def promedio_prendas_por_pedido():
    pg_cursor.execute(promedio_prendas_por_pedido_query)
    result = pg_cursor.fetchone()
    return {"data": result[0]}

# KPI 11: Total de Ventas del Mes Actual
@app.get("/kpi/total_ventas_mes_actual")
async def total_ventas_mes_actual():
    pg_cursor.execute(total_ventas_mes_actual_query)
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

@app.get("/kpi/ordernes_ultimo_mes")
async def ordenes_ultimo_mes():
    query = """
    SELECT 
        COUNT(*) AS total_orders
    FROM 
        orders
    WHERE 
        order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND
        order_date < DATE_TRUNC('month', CURRENT_DATE);
    """
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()
    return {"data": result[0]}




# Cerrar la conexión a PostgreSQL al finalizar
@app.on_event("shutdown")
async def shutdown_event():
    pg_cursor.close()
    pg_conn.close()