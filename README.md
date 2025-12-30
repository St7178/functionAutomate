# Veeam Inventory Transformer API

API REST para combinar inventarios de Veeam One (Hyper-V + VMware).

## Descripción

Esta API recibe 2 archivos Excel de inventario de Veeam One y los combina en un único archivo consolidado con:
- **Consolidado_VMs**: Todas las máquinas virtuales
- **Consolidado_Hosts**: Todos los hosts de virtualización

## Endpoints

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/` | Información de la API |
| GET | `/health` | Health check |
| POST | `/transform` | Combinar 2 archivos de inventario |

## Uso

### POST /transform

Enviar 2 archivos Excel via `multipart/form-data`:

```bash
curl -X POST https://tu-app.onrender.com/transform \
  -F "file1=@Inventory_triara_2025_12_29.xlsx" \
  -F "file2=@Inventory_2025_12_29.xlsx" \
  --output Inventory_Merged.xlsx
```

### Respuesta

Retorna un archivo Excel (`Inventory_Merged_TIMESTAMP.xlsx`) con las hojas:
- `Consolidado_VMs`: VMs de ambos archivos
- `Consolidado_Hosts`: Hosts de ambos archivos

## Despliegue en Render

1. Crear repositorio en GitHub con estos archivos
2. En Render.com → New Web Service
3. Conectar repositorio de GitHub
4. Configurar:
   - **Runtime**: Python
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`
5. Deploy

## Integración con Power Automate

1. Usar acción **HTTP** con:
   - Método: `POST`
   - URI: `https://tu-app.onrender.com/transform`
   - Body: Archivos adjuntos como `multipart/form-data`

2. El response será el archivo Excel combinado

## Desarrollo local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar servidor
python app.py

# API disponible en http://localhost:5000
```

## Estructura del proyecto

```
veeam-api/
├── app.py              # API Flask principal
├── requirements.txt    # Dependencias Python
├── render.yaml         # Configuración Render
├── .gitignore
└── README.md
```