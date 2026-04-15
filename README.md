# MVP_Censo

**Asistente estadístico con IA conectado a las bases de datos del Censo de Chile.**

Arquitectura híbrida SQL + RAG. El servidor solo sirve datos crudos — todo el razonamiento lo hace la IA del usuario.

> Actualizado: 15 Abril 2026

---

## Arquitectura

```
Usuario pregunta (español natural)
    │
    ▼
GPT / Claude / Cursor (razonamiento IA)  ← paga el usuario
    │
    ├── Consulta Knowledge files (RAG)
    │   └── Traduce: "Antofagasta" → región=2, "campamento" → tipo=12
    │
    ├── Genera SQL dinámico
    │
    ▼
POST /consultar → Render Server (FastAPI)  ← transporte HTTP, $0
    │
    ▼
MotherDuck (DuckDB) → ejecuta SQL → datos crudos  ← paga Fernando (~$0)
    │
    ▼
GPT recibe datos → análisis + gráficos + insights  ← paga el usuario
    │
    ▼
Respuesta ejecutiva al usuario
```

**Principio clave:** El servidor NUNCA llama a APIs de IA. Solo ejecuta SQL y devuelve JSON crudo. Esto significa que los tokens de razonamiento siempre los paga quien tiene la sesión de IA abierta, no el dueño del servidor.

---

## Stack técnico

| Componente | Función | Costo |
|-----------|---------|-------|
| GPT personalizado | Frontend IA, razonamiento, gráficos | Plan del usuario |
| Knowledge files (RAG) | Diccionarios INE, traducción códigos→etiquetas | $0 |
| FastAPI (Render) | Servidor API, ejecuta SQL | $0-7/mes |
| MotherDuck (DuckDB) | Data warehouse, 18M+ registros Censo | ~$0/mes |
| Code Interpreter | Gráficos matplotlib dentro de ChatGPT | Plan del usuario |
| GitHub | Repositorio código, auto-deploy a Render | $0 |

---

## Bases de datos en MotherDuck

### CENSO (2017 + 2024)
- `CENSO.main.personas_censo_2017` / `personas_censo_2024`
- `CENSO.main.hogares_censo_2017` / `hogares_censo_2024`
- `CENSO.main.viviendas_censo_2017` / `viviendas_censo_2024`

### CASEN (2022 + 2024)
- `Casen.main.casen_2022` / `casen_2024`
- `Casen.main.casen_2022_provincia_comuna` / `casen_2024_provincia_comuna`

### Estudio Etnográfico UAI (702 registros, 477 variables)
- `"Estudio Etnográfico".main.Estudio_Etnográfico`
- `"Estudio Etnográfico".main.variable_labels`
- `"Estudio Etnográfico".main.value_labels`

---

## API Endpoints

**URL:** `https://mvp-censo.onrender.com`

| Método | Ruta | Descripción | Auth |
|--------|------|-------------|------|
| GET | `/` | Política de privacidad | Público |
| GET | `/health` | Health check | Público |
| POST | `/consultar` | Ejecutar query SQL en MotherDuck | API Key |
| POST | `/graficar` | Generar gráfico (deprecado → usar Code Interpreter) | API Key |
| POST | `/exportar_excel` | Exportar resultados a Excel | API Key |
| POST | `/exportar_csv` | Exportar resultados a CSV | API Key |

**Auth:** Header `X-API-KEY` con token configurado en variables de entorno.

---

## Variables de entorno (Render)

| Variable | Descripción |
|----------|-------------|
| `MOTHERDUCK_TOKEN` | Token OAuth de MotherDuck |
| `CHATGPT_API_KEY` | API Key para autenticación del GPT |

---

## Protecciones del GPT

- **Anti-alucinación:** Nunca inventa datos sin consultar la base. Prohibido usar conocimiento general.
- **Auto-retry:** Si la API falla (cold start de Render), reintenta automáticamente hasta 3 veces.
- **Gráficos:** Usa Code Interpreter (matplotlib), no el endpoint /graficar. Estilo seaborn, DPI 150, paleta profesional.
- **Formato:** Hallazgo principal primero, 3-5 insights, fuente citada, estilo ejecutivo.

---

## Modelo de costos

| Concepto | Quién paga |
|----------|-----------|
| Tokens IA (razonamiento) | El usuario (su plan ChatGPT/Claude) |
| Transporte HTTP (Action/MCP) | $0 |
| Render hosting | Fernando ($0-7/mes) |
| MotherDuck queries | Fernando (~$0/mes) |
| API OpenAI (API key) | **$0 — el servidor NO la usa** |

---

## Roadmap

1. ✅ GPT + Action REST (estado actual)
2. → Fix PIA RAG: eliminar doble costo de IA
3. → Render Starter ($7/mes) para eliminar cold start
4. 🔮 MCP Server productizado (venta a clientes externos)
5. 🔮 Escalar bases: SII, Banco Central, SEIA, Diario Oficial

---

## Autor

**Fernando Estay** — Consultor independiente, Santiago de Chile.

Contacto: fernando.estay@gmail.com
