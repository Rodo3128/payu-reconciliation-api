import requests
import pandas as pd
import time
import os
import re
import traceback
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ==============================================================================
# --- 1. CONFIGURACIÓN CENTRALIZADA (Uso de Variables de Entorno) ---
# ==============================================================================
load_dotenv()

CONFIG = {
    # --- Configuración de API PayU ---
    "payu_user": os.getenv('PAYU_USER'),
    "payu_pass": os.getenv('PAYU_PASS'),
    "payu_merchant_id": os.getenv('PAYU_MERCHANT_ID'),
    "payu_account_id": os.getenv('PAYU_ACCOUNT_ID'),
    "payu_login_url": "https://api.payulatam.com/secure-api/authorization/login",
    "payu_api_base_url": "https://api.payulatam.com/secure-api",

    # --- Configuración de Reporte ---
    "days_to_fetch": 15,
    "temp_folder": os.getenv('TEMP_FOLDER', './temp_reports'),

    # --- Conexión a SQL Server ---
    "db_server": os.getenv('DB_SERVER'),
    "db_database": os.getenv('DB_DATABASE'),
    "db_table_name": "PayUReport",
    "db_schema": "dbo",

    # --- Mapeo Lógico de Columnas (Estructura de la Tabla) ---
    "target_columns": [
        'Id Transacción', 'Id Orden', 'Fecha de creación', 'Última actualización', 'Referencia',
        'Descripción', 'Nombre del pagador', 'Email del comprador', 'Valor original', 'Moneda original',
        'Valor procesado', 'Moneda procesada', 'Estado de orden', 'Medio de pago',
        'Tipo de tarjeta de crédito', 'Número visible tarjeta de crédito', 'Banco emisor',
        'Tipo de transacción', 'Estado de transacción', 'Código de respuesta',
        'Número de cuotas totales', 'Código de trazabilidad', 'id aliado'
    ],
    "unique_id_column": 'Id Transacción',
    "status_column": 'Estado de transacción'
}

# ==============================================================================
# --- 2. CLASE PARA DESCARGAR REPORTES DE PAYU (Arquitectura OOP) ---
# ==============================================================================

class PayUReportDownloader:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.session = requests.Session()
        # Simulación de Navegador para evitar bloqueos
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Origin': 'https://merchants.payulatam.com',
            'Referer': 'https://merchants.payulatam.com/'
        })
        self.jwt_token = None

    def login(self):
        print("[AUTH] Iniciando sesión en PayU...")
        payload = {"login": self.username, "password": self.password, "captchaResponse": ""}
        try:
            response = self.session.post(CONFIG["payu_login_url"], json=payload)
            response.raise_for_status()
            self.jwt_token = response.headers.get('jwt_auth')
            if not self.jwt_token: raise ValueError("Token JWT no encontrado.")
            self.session.headers.update({
                'Authorization': f'Bearer {self.jwt_token}', 
                'accountId': CONFIG["payu_account_id"]
            })
            print("   -> Login exitoso.")
            return True
        except Exception as e:
            print(f"   [ERROR] Login: {e}")
            return False

    def switch_merchant(self):
        print("[AUTH] Seleccionando Merchant ID...")
        url = f"{CONFIG['payu_api_base_url']}/authorization/users/switch-merchant/{CONFIG['payu_merchant_id']}"
        payload = {"merchantId": int(CONFIG['payu_merchant_id'])}
        try:
            response = self.session.put(url, json=payload)
            response.raise_for_status()
            print(f"   -> Merchant {CONFIG['payu_merchant_id']} activo.")
            return True
        except Exception as e:
            print(f"   [ERROR] Switch Merchant: {e}")
            return False

    def get_report(self, start_date, end_date, output_filepath):
        print(f"\n[REPORT] Solicitando reporte: {start_date} al {end_date}")
        if not self.login() or not self.switch_merchant(): return None

        url = f"{CONFIG['payu_api_base_url']}/merchant-reports/reports/order/load-csv/{CONFIG['payu_account_id']}/es_co"
        params = {
            'fromDate': start_date.replace('-', '/'), 
            'timeZone': 'America/Bogota', 
            'toDate': end_date.replace('-', '/')
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            file_name = response.text.strip('"')
            print(f"   -> Archivo en cola: {file_name}")
        except Exception as e:
            print(f"   [ERROR] Solicitud CSV: {e}"); return None

        # Lógica de Polling (Esperar a que el servidor procese el CSV)
        print("[PROCESS] Esperando disponibilidad del archivo...")
        check_url = f"{CONFIG['payu_api_base_url']}/merchant-reports/reports/order/check-csv"
        for attempt in range(20):
            time.sleep(5)
            response = self.session.head(check_url, params={'fileName': file_name})
            if response.status_code == 200:
                print("   -> ¡Archivo listo para descarga!"); break
            else:
                print(f"   -> Intento {attempt + 1}: Procesando todavía...")
        else:
            print("   [ERROR] Tiempo de espera agotado."); return None

        # Descarga final
        print("[DOWNLOAD] Descargando CSV...")
        download_url = f"{CONFIG['payu_api_base_url']}/merchant-reports/reports/order/download-csv"
        try:
            response = self.session.get(download_url, params={'fileName': file_name})
            response.raise_for_status()
            with open(output_filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"   -> Reporte guardado: {output_filepath}")
            return output_filepath
        except Exception as e:
            print(f"   [ERROR] Descarga: {e}"); return None

# ==============================================================================
# --- 3. FUNCIONES DE TRANSFORMACIÓN Y CARGA A SQL (DATA ENGINEERING) ---
# ==============================================================================

def prepare_dataframe(df, db_columns):
    print("[TRANSFORM] Limpiando y normalizando tipos de datos...")
    df.columns = db_columns

    # Limpieza de espacios y formateo
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()

    # Mapeo para conversiones
    mapping = dict(zip(CONFIG['target_columns'], db_columns))

    # Conversión de Fechas
    for col_name in ['Fecha de creación', 'Última actualización']:
        db_col = mapping.get(col_name)
        if db_col in df.columns:
            df[db_col] = pd.to_datetime(df[db_col], errors='coerce', format='%d/%m/%Y %H:%M:%S', dayfirst=True)

    # Conversión de Moneda/Números
    for col_name in ['Valor original', 'Valor procesado']:
        db_col = mapping.get(col_name)
        if db_col in df.columns:
            df[db_col] = df[db_col].astype(str).str.replace(',', '.', regex=False)
            df[db_col] = pd.to_numeric(df[db_col], errors='coerce')

    # Normalización de ID como String
    id_col = mapping.get(CONFIG['unique_id_column'])
    if id_col in df.columns:
        df[id_col] = df[id_col].astype(str)

    return df

def load_to_sql(df):
    if df.empty: return

    print("\n[SQL] Iniciando fase de persistencia en SQL Server...")
    conn_str = (f"mssql+pyodbc://@{CONFIG['db_server']}/{CONFIG['db_database']}"
                "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
    engine = create_engine(conn_str)

    db_columns = list(df.columns)
    mapping = dict(zip(CONFIG['target_columns'], db_columns))
    unique_col = mapping[CONFIG['unique_id_column']]
    status_col = mapping[CONFIG['status_column']]
    table_fqn = f"[{CONFIG['db_schema']}].[{CONFIG['db_table_name']}]"

    # 1. Identificar registros nuevos vs actualizaciones
    with engine.connect() as conn:
        try:
            existentes = pd.read_sql(f'SELECT "{unique_col}", "{status_col}" FROM {table_fqn}', conn)
            existentes[unique_col] = existentes[unique_col].astype(str)
        except:
            existentes = pd.DataFrame(columns=[unique_col, status_col])

    df_merged = df.merge(existentes, on=unique_col, how='left', suffixes=('', '_db'))
    nuevos = df_merged[df_merged[f'{status_col}_db'].isna()].drop(columns=[f'{status_col}_db'])
    actualizables = df_merged[
        (df_merged[f'{status_col}_db'].notna()) & (df_merged[status_col] != df_merged[f'{status_col}_db'])
    ].drop(columns=[f'{status_col}_db'])

    # 2. Ejecutar transacciones
    with engine.begin() as conn:
        if not nuevos.empty:
            print(f"   -> Insertando {len(nuevos)} nuevos registros...")
            nuevos.to_sql(CONFIG['db_table_name'], con=conn, schema=CONFIG['db_schema'], if_exists='append', index=False, chunksize=500)

        if not actualizables.empty:
            print(f"   -> Actualizando {len(actualizables)} registros (cambio de estado)...")
            # Generar consulta de UPDATE dinámica y segura (prevención de inyección)
            update_cols = [c for c in db_columns if c != unique_col]
            set_clause = ", ".join([f'"{c}" = :{re.sub(r"[^a-zA-Z0-9_]", "", c)}' for c in update_cols])
            where_clause = f'"{unique_col}" = :{re.sub(r"[^a-zA-Z0-9_]", "", unique_col)}'
            
            update_query = text(f'UPDATE {table_fqn} SET {set_clause} WHERE {where_clause}')
            
            params = []
            for record in actualizables.to_dict('records'):
                params.append({re.sub(r'[^a-zA-Z0-9_]', '', k): v for k, v in record.items()})
            
            conn.execute(update_query, params)

    print("[SQL] Carga finalizada con éxito.")

# ==============================================================================
# --- 4. ORQUESTADOR PRINCIPAL ---
# ==============================================================================

def run_pipeline():
    start_time = time.time()
    print(f"\n{'='*50}\nPIPELINE RECONCILIACIÓN PAYU - {date.today()}\n{'='*50}")

    # Preparar entorno
    os.makedirs(CONFIG["temp_folder"], exist_ok=True)
    today = date.today()
    date_from = (today - timedelta(days=CONFIG["days_to_fetch"])).strftime('%Y-%m-%d')
    date_to = today.strftime('%Y-%m-%d')
    temp_path = os.path.join(CONFIG["temp_folder"], f"payu_report_{today:%Y%m%d}.csv")

    try:
        # 1. Validar esquema destino
        conn_str = (f"mssql+pyodbc://@{CONFIG['db_server']}/{CONFIG['db_database']}"
                    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            db_cols = list(conn.execute(text(f"SELECT TOP 0 * FROM [{CONFIG['db_schema']}].[{CONFIG['db_table_name']}]")).keys())

        # 2. Descargar y procesar
        downloader = PayUReportDownloader(CONFIG["payu_user"], CONFIG["payu_pass"])
        if not downloader.get_report(date_from, date_to, temp_path):
            return

        with open(temp_path, 'r', encoding='utf-8') as f:
            df_raw = pd.read_csv(f, sep=';', dtype=str).dropna(how='all')

        if not df_raw.empty:
            df_final = prepare_dataframe(df_raw, db_cols)
            load_to_sql(df_final)
        else:
            print("[INFO] No se encontraron transacciones en el periodo.")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        traceback.print_exc()
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
            print("[CLEANUP] Archivos temporales eliminados.")

    print(f"\n{'='*50}\nPROCESO COMPLETADO EN {time.time() - start_time:.2f}s\n{'='*50}")

if __name__ == "__main__":
    # Verificación de entorno antes de arrancar
    if not CONFIG["payu_user"] or not CONFIG["payu_pass"]:
        print("\n[ERROR] Variables de entorno no configuradas. Revisa tu archivo .env")
    else:
        run_pipeline()