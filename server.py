import os
import io
import base64
import duckdb
import uvicorn
import pandas as pd
import plotly.express as px

from typing import Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field


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

if not MOTHERDUCK_TOKEN:
    raise RuntimeError("Falta la variable de entorno MOTHERDUCK_TOKEN")

# Cambia CENSO si tu base tiene otro nombre en MotherDuck
con = duckdb.connect(f"md:CENSO?motherduck_token={MOTHERDUCK_TOKEN}")


# 4. Modelos de Datos
class SQLQuery(BaseModel):
    consulta_sql: str = Field(..., description="Consulta SQL a ejecutar")


class ChartQuery(BaseModel):
    consulta_sql: str = Field(..., description="Consulta SQL que devuelve los datos del gráfico")
    chart_type: str = Field(..., description="Tipos soportados: bar, line, pie, scatter, histogram, box")
    x: str = Field(..., description="Campo para eje X o categoría principal")
    y: Optional[str] = Field(None, description="Campo para eje Y o valores")
    color: Optional[str] = Field(None, description="Campo opcional para color/segmentación")
    title: Optional[str] = Field("Gráfico", description="Título del gráfico")
    output: Optional[str] = Field(
        "json",
        description="Salida: json, html, png_base64"
    )


# 5. Ruta Pública para Privacidad
@app.get("/")
def politica_privacidad():
    return {
        "Privacy Policy": (
            "Esta es una API privada para consultas estadísticas del Censo. "
            "No recopila, almacena ni comparte datos personales."
        )
    }


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


# 6. Utilidades
def ejecutar_sql_dataframe(consulta_sql: str) -> pd.DataFrame:
    try:
        df = con.execute(consulta_sql).df()
        return df
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error en SQL: {str(e)}")


def construir_figura(df: pd.DataFrame, query: ChartQuery):
    chart_type = query.chart_type.lower().strip()

    if df.empty:
        raise HTTPException(status_code=400, detail="La consulta no devolvió datos.")

    if query.x not in df.columns:
        raise HTTPException(status_code=400, detail=f"La columna x='{query.x}' no existe en el resultado.")
    if query.y and query.y not in df.columns:
        raise HTTPException(status_code=400, detail=f"La columna y='{query.y}' no existe en el resultado.")
    if query.color and query.color not in df.columns:
        raise HTTPException(status_code=400, detail=f"La columna color='{query.color}' no existe en el resultado.")

    if chart_type == "bar":
        if not query.y:
            raise HTTPException(status_code=400, detail="El gráfico bar requiere el campo 'y'.")
        fig = px.bar(df, x=query.x, y=query.y, color=query.color, title=query.title)

    elif chart_type == "line":
        if not query.y:
            raise HTTPException(status_code=400, detail="El gráfico line requiere el campo 'y'.")
        fig = px.line(df, x=query.x, y=query.y, color=query.color, title=query.title)

    elif chart_type == "pie":
        if not query.y:
            raise HTTPException(status_code=400, detail="El gráfico pie requiere el campo 'y'.")
        fig = px.pie(df, names=query.x, values=query.y, color=query.color, title=query.title)

    elif chart_type == "scatter":
        if not query.y:
            raise HTTPException(status_code=400, detail="El gráfico scatter requiere el campo 'y'.")
        fig = px.scatter(df, x=query.x, y=query.y, color=query.color, title=query.title)

    elif chart_type == "histogram":
        fig = px.histogram(df, x=query.x, y=query.y, color=query.color, title=query.title)

    elif chart_type == "box":
        if not query.y:
            raise HTTPException(status_code=400, detail="El gráfico box requiere el campo 'y'.")
        fig = px.box(df, x=query.x, y=query.y, color=query.color, title=query.title)

    else:
        raise HTTPException(
            status_code=400,
            detail="Tipo de gráfico no soportado. Usa: bar, line, pie, scatter, histogram o box."
        )

    fig.update_layout(template="plotly_white")
    return fig


# 7. Endpoint Principal SQL
@app.post("/consultar")
async def ejecutar_consulta(
    query: SQLQuery,
    authenticated: str = Depends(validate_api_key)
):
    df = ejecutar_sql_dataframe(query.consulta_sql)
    return df.to_dict(orient="records")


# 8. Endpoint para gráficos
@app.post("/graficar")
async def graficar(
    query: ChartQuery,
    authenticated: str = Depends(validate_api_key)
):
    df = ejecutar_sql_dataframe(query.consulta_sql)
    fig = construir_figura(df, query)

    output = (query.output or "json").lower().strip()

    if output == "html":
        return HTMLResponse(content=fig.to_html(full_html=False, include_plotlyjs="cdn"))

    if output == "png_base64":
        try:
            img_bytes = fig.to_image(format="png")
            img_b64 = base64.b64encode(img_bytes).decode("utf-8")
            return {"image_base64": img_b64}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error al exportar PNG: {str(e)}")

    if output == "json":
        return JSONResponse(content=fig.to_plotly_json())

    raise HTTPException(status_code=400, detail="Output no soportado. Usa: json, html o png_base64.")


# 9. Endpoint para exportar resultados a Excel
@app.post("/exportar_excel")
async def exportar_excel(
    query: SQLQuery,
    authenticated: str = Depends(validate_api_key)
):
    df = ejecutar_sql_dataframe(query.consulta_sql)

    if df.empty:
        raise HTTPException(status_code=400, detail="La consulta no devolvió datos.")

    output = io.BytesIO()

    try:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="resultados")

        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=resultados.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al exportar Excel: {str(e)}")


# 10. Endpoint para exportar resultados a CSV
@app.post("/exportar_csv")
async def exportar_csv(
    query: SQLQuery,
    authenticated: str = Depends(validate_api_key)
):
    df = ejecutar_sql_dataframe(query.consulta_sql)

    if df.empty:
        raise HTTPException(status_code=400, detail="La consulta no devolvió datos.")

    output = io.StringIO()

    try:
        df.to_csv(output, index=False)
        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=resultados.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al exportar CSV: {str(e)}")


# 11. Inicio del Servidor
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
