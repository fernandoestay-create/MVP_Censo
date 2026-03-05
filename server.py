import os
import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

# 1. Configuración de la Aplicación
app = FastAPI(title="API Censo 2024 - Fernando Estay")

# 2. Configuración de Seguridad (API KEY)
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

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
con = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

# 4. Modelo de Datos para la Consulta
class SQLQuery(BaseModel):
    consulta_sql: str

# ---> ESTA ES LA ÚNICA PIEZA NUEVA (Ruta pública para OpenAI sin API Key) <---
@app.get("/")
def politica_privacidad():
    """Ruta libre para que OpenAI valide la política y permita compartir el GPT."""
    return {
        "Privacy Policy": "Esta es una API privada para consultas estadisticas del Censo. No recopila, almacena ni comparte datos personales."
    }
# ------------------------------------------------------------------------------

# 5. Endpoint Principal
@app.post("/consultar")
async def ejecutar_consulta(query: SQLQuery, authenticated: str = Depends(validate_api_key)):
    """
    Recibe una consulta SQL, la ejecuta en MotherDuck y devuelve los resultados.
    Requiere autenticación mediante el encabezado X-API-KEY.
    """
    try:
        df = con.execute(query.consulta_sql).df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error en SQL: {str(e)}")

# 6. Inicio del Servidor
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
