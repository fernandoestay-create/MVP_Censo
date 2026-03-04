from mcp.server.fastmcp import FastMCP
import duckdb
import os

# Inicializamos el servidor MCP
mcp = FastMCP("CensoEstadistico")

# Conectamos a MotherDuck usando el token que guardaremos en las variables de entorno
# Render se encargará de inyectar la variable MOTHERDUCK_TOKEN por seguridad
token = os.environ.get("MOTHERDUCK_TOKEN")
con = duckdb.connect(f'md:?motherduck_token={token}')

@mcp.tool()
def consultar_censo(consulta_sql: str) -> str:
    """
    Ejecuta una consulta SQL en MotherDuck sobre las tablas del Censo.
    Tablas disponibles:
    - [AQUÍ PONDREMOS EL NOMBRE DE TUS TABLAS]
    """
    try:
        # Ejecuta la consulta y convierte el resultado a formato JSON de texto
        resultado = con.execute(consulta_sql).df()
        return resultado.to_json(orient="records")
    except Exception as e:
        return f"Error ejecutando SQL: {str(e)}"

if __name__ == "__main__":
    # Inicia el servidor MCP preparado para conexiones externas (útil para Render)
    mcp.run()
