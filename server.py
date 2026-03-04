from mcp.server.fastmcp import FastMCP
import duckdb
import os

# Inicializamos el servidor MCP
mcp = FastMCP("CensoEstadistico")

# Conectamos a MotherDuck usando el token que guardaremos en Render
token = os.environ.get("MOTHERDUCK_TOKEN")
con = duckdb.connect(f'md:?motherduck_token={token}')

@mcp.tool()
def consultar_censo(consulta_sql: str) -> str:
    """
    Ejecuta una consulta SQL en MotherDuck sobre los datos estadísticos del Censo de Chile.
    
    Tablas disponibles estimadas:
    - microdato_censo2017_categorias
    - Microdato_Censo2017_Hogares
    - Microdato_Censo2017_Viviendas
    - microdato_censo2017_areas
    - hogares_censo2024
    - viviendas_censo2024
    
    INSTRUCCIÓN PARA EL LLM: Si no conoces las columnas exactas de una tabla, 
    ejecuta primero 'DESCRIBE nombre_tabla;' o 'SHOW TABLES;' para entender 
    la estructura de los datos antes de realizar el cálculo estadístico final.
    """
    try:
        # Ejecuta la consulta y convierte el resultado a formato JSON de texto
        resultado = con.execute(consulta_sql).df()
        return resultado.to_json(orient="records")
    except Exception as e:
        return f"Error ejecutando SQL: {str(e)}"

if __name__ == "__main__":
    # Inicia el servidor MCP en modo web (SSE) para Render
    mcp.run(transport='sse', host='0.0.0.0', port=int(os.environ.get("PORT", 8000)))
