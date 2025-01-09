# FILE: database.py
from pymongo import MongoClient
import psycopg2
import json
from bson import ObjectId, DBRef
from db import get_mongo_connection


# Configurar la conexión a MongoDB
db=get_mongo_connection()

# Función para obtener todos los datos de una colección
def get_all_from_collection(collection_name):
    collection = db[collection_name]
    data = list(collection.find())
    return data

# Funciones específicas para cada colección
def get_all_orders():
    return get_all_from_collection("orders")

def get_all_order_changes():
    return get_all_from_collection("order_changes")

def get_all_customers():
    return get_all_from_collection("customer")

def get_all_garments():
    return get_all_from_collection("garments")

def get_all_measurements():
    return get_all_from_collection("measurements")

def get_all_notes():
    return get_all_from_collection("notes")

def get_all_detail_notes():
    return get_all_from_collection("detail_notes")

def get_all_raw_materials():
    return get_all_from_collection("rawmaterial")

def get_all_stores():
    return get_all_from_collection("store")

def get_all_inventories():
    return get_all_from_collection("inventory")

# Función para convertir ObjectId y DBRef a cadena
def convert_object_id(data):
    if isinstance(data, list):
        return [convert_object_id(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_object_id(value) for key, value in data.items()}
    elif isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, DBRef):
        return str(data.id)
    else:
        return data

# Función para mapear los nombres de las columnas de MongoDB a PostgreSQL
def map_columns(record, table):
    column_mapping = {
        "orders": {"_id": "id", "customerId": "customer_id", "orderDate": "order_date", "totalPrice": "total_price", "orderItems": "order_items"},
        "order_changes": {"_id": "id", "orderId": "order_id", "changeDate": "change_date", "requestedBy": "requested_by"},
        "customers": {"_id": "id", "firstName": "first_name", "lastName": "last_name", "birthDate": "birth_date"},
        "garments": {"_id": "id", "basePrice": "base_price", "image": "imageurl"},
        "measurements": {"_id": "id", "orderItemId": "order_item_id", "measurementData": "measurement_data"},
        "notes": {"_id": "id", "totalAmount": "total_amount", "detailNotes": "detail_notes", "storeId": "store_id"},
        "detail_notes": {"_id": "id", "rawMaterialId": "raw_material_id"},
        "raw_materials": {"_id": "id", "name": "name", "unit": "unit"},
        "stores": {"_id": "id"},
        "inventory": {"_id": "id", "rawMaterialId": "raw_material_id", "storeId": "store_id"}
    }
    valid_columns = column_mapping.get(table, {}).values()
    mapped_record = {}
    for key, value in record.items():
        mapped_key = column_mapping.get(table, {}).get(key, key)
        if mapped_key in valid_columns:
            mapped_record[mapped_key] = value
    return mapped_record

# Función para transferir datos a PostgreSQL
def transfer_data_to_postgres(pg_conn, pg_cursor):
    try:
        collections = {
            "orders": get_all_orders,
            "order_changes": get_all_order_changes,
            "customers": get_all_customers,
            "garments": get_all_garments,
            "measurements": get_all_measurements,
            "notes": get_all_notes,
            "detail_notes": get_all_detail_notes,
            "raw_materials": get_all_raw_materials,
            "stores": get_all_stores,
            "inventory": get_all_inventories
        }

        for table, get_data_func in collections.items():
            data = get_data_func()
            for record in data:
                record = convert_object_id(record)
                record = map_columns(record, table)
                if '_class' in record:
                    del record['_class']
                columns = record.keys()
                values = [json.dumps(record[col]) if isinstance(record[col], (dict, list)) else record[col] for col in columns]
                
                # Verificar si el registro ya existe
                if 'id' in record:
                    check_statement = f"SELECT 1 FROM {table} WHERE id = %s"
                    pg_cursor.execute(check_statement, (record['id'],))
                    if pg_cursor.fetchone() is None:
                        insert_statement = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
                        pg_cursor.execute(insert_statement, values)
                        pg_conn.commit()

        #pg_cursor.close()
        #pg_conn.close()
    except psycopg2.Error as e:
        print(f"Error al transferir datos a PostgreSQL: {e}")
        if pg_conn:
            pg_conn.rollback()
       