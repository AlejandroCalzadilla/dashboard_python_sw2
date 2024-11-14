from py_eureka_client.eureka_client import EurekaClient
import logging

logging.basicConfig(level=logging.INFO)

async def register_to_eureka():
    # Configura el cliente de Eureka
    client = EurekaClient(
        app_name="pythonsastreria",
        instance_port=8001,
        eureka_server="http://localhost:8761/eureka",  # URL del servidor Eureka
        instance_host="localhost"  # Reemplaza con la IP o el hostname de tu servicio si es necesario
    )
    await client.start()
    logging.info("Registrado en Eureka con éxito")
    return client