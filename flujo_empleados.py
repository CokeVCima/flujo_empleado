import json 
import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from helpers.conn import get_sql_engine
from sqlalchemy import text
from airflow.providers.standard.operators.python import PythonOperator


APPSCRIPT = "https://script.google.com/macros/s/AKfycbwtntNjQutC1x_Bz02seUqFdpTJIjgMYwMJugaasbAIN_muuAcwPuKUfqC8SEGIWsz6/exec"
DB_CONN = "db_rh"
SCHEMA_DESTINO = "dbo"

def consulta_sql():
    engine = get_sql_engine(DB_CONN)
    query = '''
    SELECT TOP (1000) [NumeroEmpleado] as [Numero de Empleado]
        ,[NombreEmpleado] as [Nombre] 
        ,[ApellidoPaterno] as [Apellido Paterno]
        ,[ApellidoMaterno] as [Apellido Materno]
        ,[FechaIngresoEmpresa] as [Fecha de Ingreso]
        ,[DatosBasicos_CorreoCorporativo] as [Correo Corporativo]
        ,[DatosBasicos_FechaNacimiento] as [Fecha de Nacimiento]
        ,[CamposExtraPersona_AtributoSUCURSAL] as [Sucursal]
        ,[CamposExtraPersona_AtributoESTADO] as [Estado]
        ,[Posicion_NombrePosicion] as [Nombre Posicion]
        ,[Atributos_ClaveAtributo]as [Atributo]
    FROM [DBBI_RH].[dbo].[WB_Empleados]
    WHERE [ActivoRH] LIKE 'Activo'
    '''

    df = pd.read_sql_query(query, con=engine)
    if df.empty:
        print("dataframe vacio")
        return
    
    # Formateo preventivo para tipos de datos complejos en JSON
    df = df.astype(object).fillna("")
    for col in df.columns:
        # Si la columna tiene fechas, las formateamos a texto legible
        if "Fecha" in col or "Nacimiento" in col or "Ingreso" in col:
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else str(x))

    datos = [df.columns.tolist()] + df.values.tolist()

    payload = {
        "filas": datos
    }

    try:
        response = requests.post(
            APPSCRIPT, 
            data=json.dumps(payload), 
            headers={"Content-Type": "application/json"},
            allow_redirects=True, 
            timeout=120        
        )
        
        print(f"Código de respuesta HTTP: {response.status_code}")
        print(f"Respuesta de Apps Script: {response.text}")
        
        # Validaciones de error
        if response.status_code != 200:
            raise Exception(f"Error en la petición web: {response.text}")
            
        res_json = response.json()
        if res_json.get("status") == "error":
            raise Exception(f"Error interno en Apps Script: {res_json.get('message')}")
            
    except Exception as e:
        print(f"Error durante el envío a Google Sheets: {e}")
        raise


with DAG(
    dag_id='dag_rh_empleados_to_sheets',
    start_date=datetime(2026, 1, 1),
    schedule= '0 6 * * *',  
    catchup=False,
    tags=['rh'],
) as dag:

    
    task_enviar_datos = PythonOperator(
        task_id='extraer_y_subir_empleados',
        python_callable=consulta_sql
    )
