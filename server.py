import os
import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional

# ============================================================
# MVP_Censo - Server optimizado para cold start rapido
# Lazy imports: plotly, pandas, xlsxwriter, io, base64, duckdb
# se cargan SOLO cuando se usan, no al arrancar
# ============================================================

app = FastAPI(title="API Censo 2024 - Fernando Estay")

# --- Seguridad ---
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
CHATGPT_API_KEY = os.environ.get("CHATGPT_API_KEY")

async def validate_api_key(api_key: str = Depends(api_key_header)):
    if api_key == CHATGPT_API_KEY:
        return api_key
    raise HTTPException(status_code=403, detail="No autorizado: API Key invalida o ausente.")

# --- Conexion LAZY a MotherDuck ---
# NO se conecta al arrancar. Se conecta en la primera query.
# Esto reduce el cold start en ~10-15 segundos.
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
_con = None

def get_connection():
    global _con
    if _con is None:
        import duckdb
        _con = duckdb.connect(f'md:CENSO?motherduck_token={MOTHERDUCK_TOKEN}')
    return _con

# --- Modelos ---
class SQLQuery(BaseModel):
    consulta_sql: str

class GraficarRequest(BaseModel):
    consulta_sql: str
    tipo_grafico: Optional[str] = Field(default="bar", description="bar, line, pie, scatter, histogram")
    titulo: Optional[str] = Field(default="Grafico", description="Titulo del grafico")
    eje_x: Optional[str] = Field(default=None, description="Columna para eje X")
    eje_y: Optional[str] = Field(default=None, description="Columna para eje Y")
    color: Optional[str] = Field(default=None, description="Columna para color/agrupacion")

class ExportRequest(BaseModel):
    consulta_sql: str
    nombre_archivo: Optional[str] = Field(default="export", description="Nombre del archivo sin extension")

# --- Endpoints publicos (sin auth, respuesta inmediata) ---

@app.get("/")
def politica_privacidad():
    return {
        "Privacy Policy": "Esta es una API privada para consultas estadisticas del Censo. No recopila, almacena ni comparte datos personales."
    }

@app.get("/health")
def healthcheck():
    """Health check rapido - NO toca MotherDuck para responder al instante."""
    return {"status": "ok"}

# --- Endpoint principal: consultar ---

@app.post("/consultar")
async def ejecutar_consulta(query: SQLQuery, authenticated: str = Depends(validate_api_key)):
    try:
        import pandas as pd
        con = get_connection()
        df = con.execute(query.consulta_sql).df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error en SQL: {str(e)}")

# --- Endpoint: graficar ---

@app.post("/graficar")
async def graficar(request: GraficarRequest, authenticated: str = Depends(validate_api_key)):
    try:
        import pandas as pd
        import plotly.express as px
        import base64
        import io

        con = get_connection()
        df = con.execute(request.consulta_sql).df()

        if df.empty:
            raise HTTPException(status_code=400, detail="La consulta no devolvio resultados.")

        eje_x = request.eje_x or df.columns[0]
        eje_y = request.eje_y or (df.columns[1] if len(df.columns) > 1 else df.columns[0])

        chart_functions = {
            "bar": px.bar,
            "line": px.line,
            "pie": px.pie,
            "scatter": px.scatter,
            "histogram": px.histogram,
        }

        chart_func = chart_functions.get(request.tipo_grafico, px.bar)

        if request.tipo_grafico == "pie":
            fig = chart_func(df, names=eje_x, values=eje_y, title=request.titulo, color=request.color)
        else:
            fig = chart_func(df, x=eje_x, y=eje_y, title=request.titulo, color=request.color)

        fig.update_layout(template="plotly_white")

        img_bytes = fig.to_image(format="png", width=900, height=500, scale=2)
        img_base64 = base64.b64encode(img_bytes).decode("utf-8")

        return {
            "imagen_base64": img_base64,
            "datos": df.to_dict(orient="records"),
            "resumen": {
                "filas": len(df),
                "columnas": list(df.columns),
                "tipo_grafico": request.tipo_grafico,
                "titulo": request.titulo
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al graficar: {str(e)}")

# --- Endpoint: exportar Excel ---

@app.post("/exportar_excel")
async def exportar_excel(request: ExportRequest, authenticated: str = Depends(validate_api_key)):
    try:
        import pandas as pd
        import io

        con = get_connection()
        df = con.execute(request.consulta_sql).df()

        if df.empty:
            raise HTTPException(status_code=400, detail="La consulta no devolvio resultados.")

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Datos')
            workbook = writer.book
            worksheet = writer.sheets['Datos']
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#2563EB',
                'font_color': '#FFFFFF',
                'border': 1
            })
            for col_num, value in enumerate(df.columns):
                worksheet.write(0, col_num, value, header_format)
                max_len = max(df[value].astype(str).map(len).max(), len(str(value))) + 2
                worksheet.set_column(col_num, col_num, min(max_len, 40))

        buffer.seek(0)
        filename = f"{request.nombre_archivo}.xlsx"

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al exportar Excel: {str(e)}")

# --- Endpoint: exportar CSV ---

@app.post("/exportar_csv")
async def exportar_csv(request: ExportRequest, authenticated: str = Depends(validate_api_key)):
    try:
        import pandas as pd
        import io

        con = get_connection()
        df = con.execute(request.consulta_sql).df()

        if df.empty:
            raise HTTPException(status_code=400, detail="La consulta no devolvio resultados.")

        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        filename = f"{request.nombre_archivo}.csv"

        return StreamingResponse(
            iter([buffer.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al exportar CSV: {str(e)}")

# --- Inicio ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
