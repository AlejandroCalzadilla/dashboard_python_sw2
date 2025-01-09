
import requests
from fastapi import HTTPException

async def handle_query(request: QueryRequest):
    url = "http://34.55.49.4:27017:8080/graphql"  # Cambia esto a la URL del otro servicio
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=request.dict(), headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code, detail=response.text)



def consumir_find_all_cliente():
    url = "http://34.55.49.4:27017:8080/graphql"  # Cambia esto a la URL del otro servicio
    headers = {"Content-Type": "application/json"}
    query = {
        "query": "{ findAllCliente { id firstName lastName ci birthDate sex telephones { number type } userid } }"
    }
    response = requests.post(url, json=query, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code, detail=response.text)