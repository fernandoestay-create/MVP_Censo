from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import os
import uvicorn

app = FastAPI(title="API Censo Estadistico")

# Conectamos a MotherDuck
token = os.environ.get("MOTHERDUCK_TOKEN")
con = duckdb.connect(f'md:?motherduck_token={token}')

# Definimos el formato que ChatGPT nos enviará
class QueryRequest(BaseModel):
    consulta_sql: str

@app.post("/consultar")
def consultar_censo(request: QueryRequest):
    """
    Ejecuta una consulta SQL en MotherDuck.
    Tablas exclusivas a utilizar: 
    - hogares_censo2024
    - personas_censo2024
    - viviendas_censo_2024
    """
    try:
        # La base de datos hace el cálculo exacto
        resultado = con.execute(request.consulta_sql).df()
        # Se devuelve en formato JSON que ChatGPT puede leer fácilmente
        return resultado.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    # Esta es la pieza clave: forzamos a que escuche en internet (0.0.0.0) 
    # y tome el puerto dinámico que Render exige.
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
