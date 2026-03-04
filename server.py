import os
import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

# 1. Configuración de la Aplicación
app = FastAPI(title="API Censo 2024 - Fernando Estay")

# 2. Configuración de Seguridad (API KEY)
# El nombre del encabezado debe coincidir exactamente con el que pongas en ChatGPT
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# Obtenemos la llave desde las variables de entorno de Render
CHATGPT_API_KEY = os.environ.get("CHATGPT_API_KEY")

async def validate_api_key(api_key: str = Depends(api_key_header)):
    if api_key == CHATGPT_API_KEY:
        return api_key
    raise HTTPException(
        status_code=403, 
        detail="No autorizado: API Key inválida o ausente."
    )

# 3. Conexión a Base de Datos (MotherDuck)
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
# Iniciamos la conexión global
con = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

# 4. Modelo de Datos para la Consulta
class SQLQuery(BaseModel):
    consulta_sql: str

# 5. Endpoint Principal
@app.post("/consultar")
async def ejecutar_consulta(query: SQLQuery, authenticated: str = Depends(validate_api_key)):
    """
    Recibe una consulta SQL, la ejecuta en MotherDuck y devuelve los resultados.
    Requiere autenticación mediante el encabezado X-API-KEY.
    """
    try:
        # Ejecutamos la consulta y convertimos a DataFrame, luego a Diccionario
        df = con.execute(query.consulta_sql).df()
        return df.to_dict(orient="records")
    except Exception as e:
        # En caso de error en el SQL, devolvemos el detalle para que el GPT pueda corregirlo
        raise HTTPException(status_code=400, detail=f"Error en SQL: {str(e)}")

# 6. Inicio del Servidor
if __name__ == "__main__":
    # Render asigna automáticamente un puerto en la variable PORT
    port = int(os.environ.get("PORT", 8000))
    # Importante: host 0.0.0.0 para que sea accesible externamente
    uvicorn.run(app, host="0.0.0.0", port=port)
