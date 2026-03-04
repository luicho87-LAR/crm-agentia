import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pdfplumber
from google import genai
import json
import os
from fpdf import FPDF
import tempfile
import urllib.parse
from sqlalchemy import create_engine, text
import io
import zipfile
import time
import re

# --- 1. SƐTIN FƆ SƐNS MASHIN ƐN PEJ ---
st.set_page_config(page_title="Agentia CRM", layout="wide", page_icon="icono_agentia.png")

# 🚨 AYD YU API KEY NA STREAMLIT SECRETS 🚨
API_KEY = st.secrets["GEMINI_API_KEY"] 
client = genai.Client(api_key=API_KEY)

# --- ✨ PREMIUM DIZAYN (UI/UX) ✨ ---
st.markdown("""
<style>
    .stApp { background-color: #f4f7f9; background-image: radial-gradient(circle at 50% 0%, #e0edfb 0%, #f4f7f9 40%); }
    div[data-testid="metric-container"] { background-color: #ffffff; border: 1px solid #e1e8ed; padding: 15px 20px; border-radius: 12px; box-shadow: 0 4px 10px rgba(0,0,0,0.04); border-top: 4px solid #0b7af0; transition: transform 0.2s ease, box-shadow 0.2s ease; }
    div[data-testid="metric-container"]:hover { transform: translateY(-3px); box-shadow: 0 8px 15px rgba(11, 122, 240, 0.1); }
    div.stButton > button[kind="primary"] { background: linear-gradient(135deg, #0b7af0 0%, #0052a3 100%); color: white; border-radius: 8px; border: none; padding: 0.6rem 1.2rem; font-weight: 600; box-shadow: 0 4px 12px rgba(11, 122, 240, 0.3); transition: all 0.3s ease; }
    div.stButton > button[kind="primary"]:hover { box-shadow: 0 6px 18px rgba(11, 122, 240, 0.5); transform: scale(1.02); }
    div[data-testid="stExpander"] { background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.03); border: 1px solid #eef2f5; overflow: hidden; margin-bottom: 1rem; }
    div[data-testid="stExpander"] details summary { background-color: #ffffff; padding: 10px; font-size: 1.05rem; color: #1e293b; }
    button[data-baseweb="tab"] { font-size: 16px; font-weight: 600; border-radius: 8px 8px 0 0; padding: 10px 16px; margin-right: 2px; }
    button[data-baseweb="tab"][aria-selected="true"] { background-color: #eaf3fc; color: #0b7af0; border-bottom: 3px solid #0b7af0; }
    div[data-baseweb="input"] > div, div[data-baseweb="select"] > div { border-radius: 8px; border: 1px solid #d1d9e0; }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- 🔐 SISTEMA DE LOGIN Y SEGURIDAD ---
if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    col_espacio1, col_login, col_espacio2 = st.columns([1, 1.5, 1])
    with col_login:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        c_izq, c_centro_img, c_der = st.columns([1, 2, 1])
        with c_centro_img:
            if os.path.exists("logo_crm.png"):
                st.image("logo_crm.png", use_container_width=True)
        st.markdown("""<h4 style='text-align: center; color: #0b7af0; font-weight: 600; letter-spacing: 1px; margin-top: -10px; margin-bottom: 30px;'>Inteligencia para vender más</h4>""", unsafe_allow_html=True)
            
        st.markdown("<h5 style='text-align: center;'>🔐 Acceso Corporativo</h5>", unsafe_allow_html=True)
        with st.form("form_login"):
            usuario = st.text_input("Usuario", placeholder="Ej. admin")
            contrasena = st.text_input("Contraseña", type="password", placeholder="••••••••")
            boton_entrar = st.form_submit_button("Iniciar Sesión ➜", type="primary", use_container_width=True)
            if boton_entrar:
                if usuario == "admin" and contrasena == "Agentia2026":
                    st.session_state['autenticado'] = True
                    st.rerun()
                else: st.error("Usuario o contraseña incorrectos.")
    st.stop()

# --- 2. CONEXIÓN A LA NUBE (SUPABASE) ---
@st.cache_resource
def init_connection():
    db_url = st.secrets["DATABASE_URL"].replace("postgres://", "postgresql://")
    return create_engine(db_url)

try:
    engine = init_connection()
except Exception as e:
    st.error("Error conectando a la base de datos. Verifica tu contraseña y enlace en los Secrets.")
    st.stop()

def inicializar_bd_completa():
    with engine.begin() as conn:
        conn.execute(text('''CREATE TABLE IF NOT EXISTS Prospectos (id SERIAL PRIMARY KEY, nombre TEXT, correo TEXT, telefono TEXT, producto TEXT, fecha_cotizacion TEXT, ejecutivo TEXT DEFAULT 'Titular (Agencia)')'''))
        conn.execute(text('''CREATE TABLE IF NOT EXISTS Clientes (rfc TEXT PRIMARY KEY, nombre TEXT, telefono TEXT, correo TEXT, fecha_nacimiento TEXT, direccion TEXT)'''))
        conn.execute(text('''CREATE TABLE IF NOT EXISTS Polizas (numero_poliza TEXT PRIMARY KEY, rfc_cliente TEXT, aseguradora TEXT, inicio_vigencia TEXT, fin_vigencia TEXT, archivo_pdf BYTEA, ejecutivo TEXT DEFAULT 'Titular (Agencia)', tipo_producto TEXT DEFAULT 'No especificado', vehiculo TEXT DEFAULT 'N/A')'''))
        conn.execute(text('''CREATE TABLE IF NOT EXISTS Recibos (id SERIAL PRIMARY KEY, numero_poliza TEXT, fecha_limite TEXT, monto TEXT, estado TEXT DEFAULT 'Pendiente')'''))
        conn.execute(text('''CREATE TABLE IF NOT EXISTS Ejecutivos (id SERIAL PRIMARY KEY, nombre TEXT UNIQUE)'''))
        
        conn.execute(text('''ALTER TABLE Polizas ADD COLUMN IF NOT EXISTS tipo_producto TEXT DEFAULT 'No especificado' '''))
        conn.execute(text('''ALTER TABLE Polizas ADD COLUMN IF NOT EXISTS vehiculo TEXT DEFAULT 'N/A' '''))
        
        res = conn.execute(text("SELECT COUNT(*) FROM Ejecutivos")).scalar()
        if res == 0:
            conn.execute(text("INSERT INTO Ejecutivos (nombre) VALUES ('Titular (Agencia)')"))

inicializar_bd_completa()

# --- 3. FUNCIONES DEL SISTEMA ---
def obtener_lista_ejecutivos():
    df = pd.read_sql_query("SELECT nombre FROM Ejecutivos ORDER BY id", engine)
    return df['nombre'].tolist()

def formato_pesos(valor):
    v = str(valor).strip()
    if v.lower() in ['nan', 'none', 'no especificado', 'no especificada', '']: return "No especificado"
    if v.startswith('$'): return v
    try:
        num = float(v.replace(',', '').replace(' ', ''))
        return f"${num:,.2f}"
    except: return f"${v}"

def extraer_texto_pdf(archivo_pdf):
    texto_completo = ""
    try:
        with pdfplumber.open(archivo_pdf) as pdf:
            for pagina in pdf.pages:
                texto_extraido = pagina.extract_text()
                if texto_extraido: texto_completo += texto_extraido + "\n"
        return texto_completo
    except: return None

def limpiar_json(texto):
    try:
        if not texto: return None
        texto_limpio = str(texto).strip()
        
        if "```json" in texto_limpio:
            texto_limpio = texto_limpio.split("```json")[1].split("```")[0]
        elif "```" in texto_limpio:
            texto_limpio = texto_limpio.split("```")[1].split("```")[0]
            
        inicio_obj = texto_limpio.find('{')
        fin_obj = texto_limpio.rfind('}') + 1
        inicio_arr = texto_limpio.find('[')
        fin_arr = texto_limpio.rfind(']') + 1
        
        if inicio_obj != -1 and fin_obj > 0 and (inicio_arr == -1 or inicio_obj < inicio_arr):
            return json.loads(texto_limpio[inicio_obj:fin_obj])
        elif inicio_arr != -1 and fin_arr > 0:
            arr = json.loads(texto_limpio[inicio_arr:fin_arr])
            if isinstance(arr, list) and len(arr) > 0: return arr[0]
        return None
    except:
        return None

PLANTILLA_IA = """
{
    "tipo_documento": "Poliza",
    "aseguradora": "Ejemplo Aseguradora",
    "numero_poliza": "123456",
    "nombre_cliente": "Juan Perez",
    "rfc_cliente": "XAXX010101000",
    "telefono": "5551234567",
    "correo": "correo@ejemplo.com",
    "inicio_vigencia": "01/01/2024",
    "fin_vigencia": "01/01/2025",
    "direccion_completa": "Calle Falsa 123",
    "tipo_producto": "Autos",
    "vehiculo": "Nissan Versa 2023",
    "fecha_limite_pago": "15/01/2024",
    "monto_a_pagar": "1500.00",
    "forma_pago": "Tarjeta de Credito",
    "fecha_nacimiento": "01/01/1990"
}
"""

def analizar_con_ia(texto_sucio):
    instruccion = f"""Eres un robot experto en seguros. Tu ÚNICA tarea es extraer información y devolverla ESTRICTAMENTE en este formato JSON, sin saludos, sin explicaciones, sin texto extra:\n{PLANTILLA_IA}\nSi un campo no aparece, pon "No especificado". Las fechas deben ser DD/MM/AAAA. Calcula fecha_nacimiento con el RFC si es posible."""
    
    # Motor de velocidad: 3 intentos rápidos por si hay un fallo de red. Sin frenos de tiempo largos.
    for intento in range(3):
        try:
            prompt_completo = f"{instruccion}\n\n--- DOCUMENTO ---\n{texto_sucio}"
            response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt_completo)
            return response.text
        except Exception as e: 
            error_str = str(e)
            if intento < 2:  
                time.sleep(2) # Pausa mínima de 2 segundos solo por estabilidad de red
                continue
            return f"ERROR_API: {error_str}"
    return "ERROR_API: Error de conexión con Google."

def guardar_poliza_bd(datos, pdf_bytes=None, ejecutivo="Titular (Agencia)"):
    if not isinstance(datos, dict):
        return f"Error de formato JSON devuelto."
        
    with engine.begin() as conn:
        try:
            tipo_doc = str(datos.get('tipo_documento', 'Poliza'))
            rfc = str(datos.get('rfc_cliente', '')).strip()
            if not rfc or rfc.lower() in ['no especificado', 'none', 'null', 'na']: rfc = f"SIN_RFC_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            num_pol = str(datos.get('numero_poliza', '')).strip()
            if not num_pol or num_pol.lower() in ['no especificado', 'none', 'null', '', 'na']: num_pol = f"POR_ASIGNAR_{datetime.now().strftime('%H%M%S')}"
            
            nom = str(datos.get('nombre_cliente', 'No especificado'))
            tel = str(datos.get('telefono', 'No especificado'))
            cor = str(datos.get('correo', 'No especificado'))
            fec = str(datos.get('fecha_nacimiento', 'No especificado'))
            direc = str(datos.get('direccion_completa', 'No especificada'))
            
            conn.execute(text("""
                INSERT INTO Clientes (rfc, nombre, telefono, correo, fecha_nacimiento, direccion) 
                VALUES (:rfc, :nom, :tel, :cor, :fec, :dir) 
                ON CONFLICT (rfc) DO UPDATE SET 
                nombre=EXCLUDED.nombre, telefono=EXCLUDED.telefono, correo=EXCLUDED.correo, direccion=EXCLUDED.direccion
            """), {"rfc": rfc, "nom": nom, "tel": tel, "cor": cor, "fec": fec, "dir": direc})
            
            prod = str(datos.get('tipo_producto', 'No especificado'))
            veh = datos.get('vehiculo', 'N/A')
            if isinstance(veh, dict): veh = " ".join([str(v) for v in veh.values()])
            elif isinstance(veh, list): veh = ", ".join([str(v) for v in veh])
            else: veh = str(veh)
            
            aseg = str(datos.get('aseguradora', 'No especificado'))
            ini = str(datos.get('inicio_vigencia', 'No especificado'))
            fin = str(datos.get('fin_vigencia', 'No especificado'))
            
            if 'poliza' in tipo_doc.lower():
                conn.execute(text("""
                    INSERT INTO Polizas (numero_poliza, rfc_cliente, aseguradora, inicio_vigencia, fin_vigencia, archivo_pdf, ejecutivo, tipo_producto, vehiculo) 
                    VALUES (:pol, :rfc, :aseg, :ini, :fin, :pdf, :ejec, :prod, :veh)
                    ON CONFLICT (numero_poliza) DO UPDATE SET 
                    inicio_vigencia=EXCLUDED.inicio_vigencia, fin_vigencia=EXCLUDED.fin_vigencia, archivo_pdf=EXCLUDED.archivo_pdf, ejecutivo=EXCLUDED.ejecutivo, tipo_producto=EXCLUDED.tipo_producto, vehiculo=EXCLUDED.vehiculo
                """), {"pol": num_pol, "rfc": rfc, "aseg": aseg, "ini": ini, "fin": fin, "pdf": pdf_bytes, "ejec": ejecutivo, "prod": prod, "veh": veh})
            else:
                conn.execute(text("""
                    INSERT INTO Polizas (numero_poliza, rfc_cliente, aseguradora, inicio_vigencia, fin_vigencia, archivo_pdf, ejecutivo, tipo_producto, vehiculo) 
                    VALUES (:pol, :rfc, :aseg, :ini, :fin, :pdf, :ejec, :prod, :veh)
                    ON CONFLICT (numero_poliza) DO NOTHING
                """), {"pol": num_pol, "rfc": rfc, "aseg": aseg, "ini": ini, "fin": fin, "pdf": pdf_bytes, "ejec": ejecutivo, "prod": prod, "veh": veh})
            
            fecha_pago = datos.get('fecha_limite_pago')
            if fecha_pago and str(fecha_pago).lower() not in ['no especificado', 'none', 'null', 'na', '']:
                monto = formato_pesos(datos.get('monto_a_pagar', 'No especificado'))
                forma_pago = str(datos.get('forma_pago', '')).lower()
                
                tarjetas_clave = ['visa', 'master', 'amex', 'tarjeta', 'credito', 'debito', 'cargo']
                if any(palabra in forma_pago for palabra in tarjetas_clave):
                    estado_recibo = 'Cargo Automático'
                else:
                    estado_recibo = 'Pendiente'
                
                res = conn.execute(text("SELECT id FROM Recibos WHERE numero_poliza=:pol AND fecha_limite=:fec"), {"pol": num_pol, "fec": str(fecha_pago)}).fetchone()
                if not res:
                    conn.execute(text("INSERT INTO Recibos (numero_poliza, fecha_limite, monto, estado) VALUES (:pol, :fec, :mon, :est)"), {"pol": num_pol, "fec": str(fecha_pago), "mon": monto, "est": estado_recibo})
            return tipo_doc
        except Exception as e:
            return f"Error SQL: {str(e)}"

def generar_pdf_con_logos(df, titulo, fecha_inicio, fecha_fin):
    pdf = FPDF(orientation='L', unit='mm', format='A4')
    pdf.add_page()
    if os.path.exists("logo_agencia.png"): pdf.image("logo_agencia.png", 10, 8, 30)
    if os.path.exists("logo_crm.png"): pdf.image("logo_crm.png", 250, 8, 30)
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, titulo, ln=1, align='C')
    pdf.set_font("Arial", "", 11)
    pdf.cell(0, 8, f"Periodo analizado: {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')}", ln=1, align='C')
    pdf.ln(10)
    if not df.empty:
        pdf.set_font("Arial", "B", 9)
        ancho_col = 277 / max(1, len(df.columns))
        for col in df.columns: pdf.cell(ancho_col, 8, str(col).encode('latin-1', 'replace').decode('latin-1')[:20], border=1, align='C')
        pdf.ln()
        pdf.set_font("Arial", "", 8)
        for index, fila in df.iterrows():
            for item in fila: pdf.cell(ancho_col, 8, str(item).encode('latin-1', 'replace').decode('latin-1')[:25], border=1, align='C')
            pdf.ln()
    fd, temp_path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd) 
    pdf.output(temp_path)
    with open(temp_path, "rb") as f: pdf_bytes = f.read()
    try: os.remove(temp_path)
    except Exception: pass 
    return pdf_bytes

# --- 4. DISEÑO DE LA PANTALLA WEB E INTEGRACIÓN DE CONFIGURACIÓN ---
col_tit, col_vacia, col_der = st.columns([5, 1, 2])
with col_tit:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<h1 style='color: #0b7af0; margin-bottom: 0px;'>Sistema de Gestión Integral</h1>", unsafe_allow_html=True)
    st.caption("Inteligencia para vender más")

with col_der:
    if os.path.exists("logo_crm.png"):
        st.image("logo_crm.png", use_container_width=True)
        
    with st.popover("⚙️ Configuración y Respaldos", use_container_width=True):
        
        with st.expander("👤 Gestión de Ejecutivos", expanded=False):
            with st.form("form_nuevo_ejecutivo", clear_on_submit=True):
                nuevo_nombre = st.text_input("Nombre completo:")
                if st.form_submit_button("➕ Agregar al Equipo", type="primary"):
                    if nuevo_nombre:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("INSERT INTO Ejecutivos (nombre) VALUES (:nom)"), {"nom": nuevo_nombre.strip()})
                            st.success(f"¡{nuevo_nombre} agregado!"); st.rerun() 
                        except Exception: st.error("Este nombre ya está registrado.")
                    else: st.warning("El campo no puede estar vacío.")
            df_equipo = pd.read_sql_query("SELECT id as ID, nombre as Nombre FROM Ejecutivos ORDER BY id", engine)
            st.dataframe(df_equipo, hide_index=True, use_container_width=True)
            
        with st.expander("📥 Importar Cartera Masiva (Excel/CSV)", expanded=False):
            df_plantilla = pd.DataFrame(columns=['Nombre_Completo', 'RFC', 'Telefono', 'Correo', 'Direccion', 'Aseguradora', 'Numero_Poliza', 'Inicio_Vigencia_DD/MM/AAAA', 'Fin_Vigencia_DD/MM/AAAA', 'Ejecutivo'])
            st.download_button(label="📥 Bajar Formato .CSV", data=df_plantilla.to_csv(index=False).encode('utf-8-sig'), file_name="Plantilla_Agentia.csv", mime='text/csv')
            
            archivo_importar = st.file_uploader("Sube tu archivo lleno (.csv o .xlsx)", type=["csv", "xlsx"])
            if archivo_importar and st.button("🚀 Iniciar Carga", type="primary"):
                with st.spinner("Sincronizando registros en la nube..."):
                    try:
                        df_import = pd.read_csv(archivo_importar) if archivo_importar.name.endswith('.csv') else pd.read_excel(archivo_importar)
                        registros = 0
                        with engine.begin() as conn:
                            for index, fila in df_import.iterrows():
                                nombre = str(fila.get('Nombre_Completo', '')).strip()
                                if nombre == 'nan' or not nombre: continue
                                rfc = str(fila.get('RFC', 'No especificado')).strip()
                                if rfc == 'nan' or not rfc: rfc = f"SIN_RFC_{index}"
                                tel = str(fila.get('Telefono', 'No especificado'))
                                correo = str(fila.get('Correo', 'No especificado'))
                                direc = str(fila.get('Direccion', 'No especificada'))
                                aseg = str(fila.get('Aseguradora', ''))
                                pol = str(fila.get('Numero_Poliza', ''))
                                ini = str(fila.get('Inicio_Vigencia_DD/MM/AAAA', 'No especificado'))
                                fin = str(fila.get('Fin_Vigencia_DD/MM/AAAA', 'No especificado'))
                                ejecutivo_excel = str(fila.get('Ejecutivo', 'Titular (Agencia)')).strip()
                                if ejecutivo_excel == 'nan' or not ejecutivo_excel: ejecutivo_excel = 'Titular (Agencia)'
                                
                                conn.execute(text("INSERT INTO Clientes (rfc, nombre, telefono, correo, fecha_nacimiento, direccion) VALUES (:rfc, :nom, :tel, :cor, 'No calculado', :dir) ON CONFLICT (rfc) DO UPDATE SET nombre=EXCLUDED.nombre, telefono=EXCLUDED.telefono, correo=EXCLUDED.correo, direccion=EXCLUDED.direccion"), {"rfc": rfc, "nom": nombre, "tel": tel, "cor": correo, "dir": direc})
                                if pol and pol != 'nan':
                                    conn.execute(text("INSERT INTO Polizas (numero_poliza, rfc_cliente, aseguradora, inicio_vigencia, fin_vigencia, ejecutivo, tipo_producto, vehiculo) VALUES (:pol, :rfc, :aseg, :ini, :fin, :ejec, 'No especificado', 'N/A') ON CONFLICT (numero_poliza) DO UPDATE SET inicio_vigencia=EXCLUDED.inicio_vigencia, fin_vigencia=EXCLUDED.fin_vigencia, ejecutivo=EXCLUDED.ejecutivo"), {"pol": pol, "rfc": rfc, "aseg": aseg, "ini": ini, "fin": fin, "ejec": ejecutivo_excel})
                                registros += 1
                        st.success(f"🎉 ¡Misión cumplida! Se migraron {registros} clientes."); st.balloons()
                    except Exception as e: st.error(f"Error técnico: {e}")

        with st.expander("☁️ Respaldo de Base de Datos", expanded=False):
            st.info("Descarga un archivo ZIP con todos tus registros actuales para respaldarlo en Google Drive.")
            if st.button("📦 Generar ZIP de Respaldo", type="primary", use_container_width=True):
                with st.spinner("Empaquetando..."):
                    try:
                        df_c = pd.read_sql_query("SELECT * FROM Clientes", engine)
                        df_p = pd.read_sql_query("SELECT numero_poliza, rfc_cliente, aseguradora, tipo_producto, vehiculo, inicio_vigencia, fin_vigencia, ejecutivo FROM Polizas", engine)
                        df_r = pd.read_sql_query("SELECT * FROM Recibos", engine)
                        df_pr = pd.read_sql_query("SELECT * FROM Prospectos", engine)
                        
                        zip_buffer = io.BytesIO()
                        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                            zip_file.writestr("1_Clientes.csv", df_c.to_csv(index=False).encode('utf-8-sig'))
                            zip_file.writestr("2_Polizas.csv", df_p.to_csv(index=False).encode('utf-8-sig'))
                            zip_file.writestr("3_Recibos.csv", df_r.to_csv(index=False).encode('utf-8-sig'))
                            zip_file.writestr("4_Prospectos.csv", df_pr.to_csv(index=False).encode('utf-8-sig'))
                        
                        st.success("¡Listo!")
                        st.download_button(
                            label="📥 Guardar Archivo ZIP",
                            data=zip_buffer.getvalue(),
                            file_name=f"Respaldo_Agentia_{datetime.now().strftime('%Y%m%d')}.zip",
                            mime="application/zip",
                            type="secondary",
                            use_container_width=True
                        )
                    except Exception as e: st.error(f"Error: {e}")

st.markdown("---")
lista_dinamica_ejecutivos = obtener_lista_ejecutivos()

pestana0, pestana1, pestana2, pestana3, pestana4, pestana5 = st.tabs([
    "📊 Dashboard VIP", "🔍 Buscador Inteligente", "📄 Lector IA Masivo", "🚦 Seguimiento Prospectos", 
    "🔔 Alertas y Cobranza", "📈 Reportes"
])

# ==========================================
# PESTAÑA 0: DASHBOARD DIRECTOR (NUEVA)
# ==========================================
with pestana0:
    st.markdown("### 📊 Tablero de Control Ejecutivo")
    
    # 1. Métricas Principales
    t_clientes = pd.read_sql_query("SELECT COUNT(*) FROM Clientes", engine).iloc[0,0]
    t_polizas = pd.read_sql_query("SELECT COUNT(*) FROM Polizas", engine).iloc[0,0]
    
    df_recibos_pendientes = pd.read_sql_query("SELECT monto FROM Recibos WHERE estado = 'Pendiente'", engine)
    suma_pendientes = 0
    for m in df_recibos_pendientes['monto']:
        try:
            val = str(m).replace('$', '').replace(',', '').strip()
            suma_pendientes += float(val) if val else 0
        except: pass
        
    c1, c2, c3 = st.columns(3)
    c1.metric("👥 Clientes Totales", t_clientes)
    c2.metric("📑 Pólizas Activas", t_polizas)
    c3.metric("💰 Cobranza Pendiente en Calle", f"${suma_pendientes:,.2f}")
    
    st.markdown("---")
    
    # --- MOTORES DE AGRUPACIÓN INTELIGENTE ---
    def normalizar_ramo(texto):
        if pd.isna(texto): return "Otro"
        t = str(texto).upper()
        if "AUTO" in t: return "Autos"
        if "MEDIC" in t or "MÉDIC" in t or "GMM" in t or "SALUD" in t: return "Gastos Médicos"
        if "VIDA" in t: return "Vida"
        if "HOGAR" in t or "RESIDENC" in t or "CASA" in t: return "Hogar"
        if "DAÑO" in t or "EMPRESA" in t: return "Daños Empresariales"
        return "Otro"

    def normalizar_aseguradora(texto):
        if pd.isna(texto) or str(texto).lower() in ['nan', 'none', '']: return "No especificada"
        t = str(texto).upper().strip()
        
        # Quitar puntos y comas
        t = t.replace('.', '').replace(',', '')
        # Quitar terminaciones legales
        t = re.sub(r'\b(S A DE C V|S A B DE C V|S A P I DE C V|S A|SAB|SAPI|DE C V|C V)\b', '', t)
        t = re.sub(r'\s+', ' ', t).strip()
        
        # Homologar marcas comerciales
        if "AXA" in t: return "AXA"
        if "GNP" in t or "NACIONAL PROVINCIAL" in t: return "GNP"
        if "QUALITAS" in t or "QUÁLITAS" in t: return "Quálitas"
        if "MAPFRE" in t: return "Mapfre"
        if "ZURICH" in t: return "Zurich"
        if "HDI" in t: return "HDI"
        if "CHUBB" in t: return "Chubb"
        if "ABA" in t: return "ABA"
        if "INBURSA" in t: return "Inbursa"
        if "BANORTE" in t: return "Banorte"
        if "ATLAS" in t: return "Atlas"
        if "AFIRME" in t: return "Afirme"
        if "GENERAL" in t: return "General de Seguros"
        if "ANA" in t: return "ANA Seguros"
        if "BUPA" in t: return "BUPA"
        if "ALLIANZ" in t: return "Allianz"
        
        return t.title() # Si es una nueva, la capitaliza bonito

    def limpiar_dinero(val):
        try:
            if val is None or str(val).lower() in ['nan', 'none', 'no especificado', '']: return 0.0
            v = str(val).replace('$', '').replace(',', '').replace(' ', '').strip()
            return float(v)
        except:
            return 0.0

    # 2. Gráficos Interactivos
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        st.markdown("##### 🚗 Pólizas por Ramo")
        df_ramos = pd.read_sql_query("SELECT tipo_producto FROM Polizas", engine)
        if not df_ramos.empty:
            df_ramos['Ramo'] = df_ramos['tipo_producto'].apply(normalizar_ramo)
            df_ramos_agrupado = df_ramos.groupby('Ramo').size().reset_index(name='Total').set_index('Ramo')
            st.bar_chart(df_ramos_agrupado, color="#0b7af0")
        else:
            st.info("Sube pólizas para ver esta gráfica.")
            
    with col_g2:
        st.markdown("##### 🏢 Prima Registrada por Aseguradora")
        # Unimos pólizas con recibos para sumar el dinero
        query_primas = """
        SELECT p.aseguradora, r.monto, r.estado 
        FROM Polizas p 
        JOIN Recibos r ON p.numero_poliza = r.numero_poliza
        """
        df_primas = pd.read_sql_query(query_primas, engine)
        if not df_primas.empty:
            df_primas['Aseguradora'] = df_primas['aseguradora'].apply(normalizar_aseguradora)
            df_primas['Prima'] = df_primas['monto'].apply(limpiar_dinero)
            
            # Agrupamos sumando todo el dinero de los recibos
            df_primas_agrupado = df_primas.groupby('Aseguradora')['Prima'].sum().reset_index().set_index('Aseguradora')
            
            # Filtramos para no mostrar compañías con $0
            df_primas_agrupado = df_primas_agrupado[df_primas_agrupado['Prima'] > 0]
            
            if not df_primas_agrupado.empty:
                st.bar_chart(df_primas_agrupado, color="#2ecc71")
            else:
                st.info("Ninguna póliza registrada tiene montos válidos aún.")
        else:
            st.info("Sube pólizas que contengan recibos para graficar la prima.")

# ==========================================
# PESTAÑA 1: BUSCADOR VIP 
# ==========================================
with pestana1:
    st.markdown("### 📇 Archivo Digital de Clientes")
    total_clientes = pd.read_sql_query("SELECT COUNT(*) FROM Clientes", engine).iloc[0,0]
    total_polizas = pd.read_sql_query("SELECT COUNT(*) FROM Polizas", engine).iloc[0,0]
    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("👥 Clientes en Cartera", total_clientes)
    col_m2.metric("📑 Pólizas Activas", total_polizas)
    col_m3.metric("📅 Actualización", datetime.now().strftime('%d/%m/%Y'))
    st.markdown("<br>", unsafe_allow_html=True)
    
    busqueda = st.text_input("🔍 Escribe el Nombre o RFC del cliente para abrir su expediente:", placeholder="Ej. Luis Alberto...")
    if busqueda:
        df_clientes = pd.read_sql_query(f"SELECT * FROM Clientes WHERE nombre ILIKE '%%{busqueda}%%' OR rfc ILIKE '%%{busqueda}%%'", engine)
        if not df_clientes.empty:
            for index, cliente in df_clientes.iterrows():
                with st.expander(f"👤 {cliente['nombre']} (RFC: {cliente['rfc']})", expanded=True):
                    col_a, col_b, col_c = st.columns(3)
                    col_a.write(f"**📞 Teléfono:** {cliente['telefono']}")
                    col_b.write(f"**✉️ Correo:** {cliente['correo']}")
                    col_c.write(f"**🎂 Nacimiento:** {cliente['fecha_nacimiento']}")
                    st.write(f"**📍 Dirección:** {cliente['direccion']}")
                    
                    with st.popover("✏️ Editar perfil (Agregar Cumpleaños)"):
                        with st.form(f"form_editar_{cliente['rfc']}"):
                            nuevo_tel = st.text_input("Teléfono", value=cliente['telefono'] if cliente['telefono'] != "No especificado" else "")
                            nuevo_correo = st.text_input("Correo", value=cliente['correo'] if cliente['correo'] != "No especificado" else "")
                            nueva_fec = st.text_input("Fecha de Nacimiento (DD/MM/AAAA)", value=cliente['fecha_nacimiento'] if cliente['fecha_nacimiento'] != "No especificado" and cliente['fecha_nacimiento'] != "No calculado" else "")
                            nueva_dir = st.text_input("Dirección", value=cliente['direccion'] if cliente['direccion'] != "No especificada" else "")
                            if st.form_submit_button("Guardar Cambios"):
                                with engine.begin() as conn:
                                    conn.execute(text("UPDATE Clientes SET telefono=:tel, correo=:cor, direccion=:dir, fecha_nacimiento=:fec WHERE rfc=:rfc"), 
                                                {"tel": nuevo_tel, "cor": nuevo_correo, "dir": nueva_dir, "fec": nueva_fec, "rfc": cliente['rfc']})
                                st.success("¡Actualizado!"); st.rerun()

                    st.markdown("#### 📑 Pólizas Activas")
                    df_polizas = pd.read_sql_query(f"SELECT aseguradora, numero_poliza, tipo_producto, vehiculo, inicio_vigencia, fin_vigencia, ejecutivo FROM Polizas WHERE rfc_cliente = '{cliente['rfc']}'", engine)
                    if not df_polizas.empty:
                        df_polizas.columns = ['Aseguradora', 'Poliza', 'Ramo', 'Vehículo', 'Inicio', 'Fin', 'Ejecutivo']
                        st.dataframe(df_polizas, use_container_width=True, hide_index=True)
                        st.markdown("**📥 Descargar Documentos Originales:**")
                        cols_descarga = st.columns(len(df_polizas))
                        for idx, poliza in df_polizas.iterrows():
                            with engine.connect() as conn:
                                pdf_data = conn.execute(text("SELECT archivo_pdf FROM Polizas WHERE numero_poliza=:pol"), {"pol": poliza['Poliza']}).fetchone()[0]
                            with cols_descarga[idx % len(cols_descarga)]: 
                                if pdf_data: st.download_button(label=f"📄 {poliza['Poliza']}", data=bytes(pdf_data), file_name=f"Doc_{poliza['Poliza'].replace('/','_')}.pdf", mime="application/pdf", key=f"dl_{poliza['Poliza']}_{idx}")
                                else: st.caption(f"🚫 Sin PDF en bóveda")
                        st.markdown("---")
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            with st.popover("➕ Cargar recibo manual"):
                                with st.form(f"form_recibo_{cliente['rfc']}"):
                                    poliza_sel = st.selectbox("Selecciona la póliza", df_polizas['Poliza'].tolist())
                                    monto_recibo = st.text_input("Monto a pagar (Ej. 1500)")
                                    fecha_recibo = st.date_input("Fecha límite de pago")
                                    if st.form_submit_button("Guardar Recibo"):
                                        monto_limpio = formato_pesos(monto_recibo)
                                        with engine.begin() as conn:
                                            conn.execute(text("INSERT INTO Recibos (numero_poliza, fecha_limite, monto, estado) VALUES (:pol, :fec, :mon, 'Pendiente')"), {"pol": poliza_sel, "fec": fecha_recibo.strftime("%d/%m/%Y"), "mon": monto_limpio})
                                        st.success("Recibo agregado"); st.rerun()
                        
                        with col_btn2:
                            with st.popover("🏷️ Clasificar Póliza / Vehículo"):
                                with st.form(f"form_etiquetas_{cliente['rfc']}"):
                                    poliza_clasificar = st.selectbox("Selecciona la póliza a clasificar", df_polizas['Poliza'].tolist())
                                    nuevo_prod = st.selectbox("Tipo de Producto", ["Autos", "Gastos Médicos Mayores", "Vida", "Daños Empresariales", "Hogar", "Otro"])
                                    nuevo_veh = st.text_input("Etiqueta del Vehículo (Solo si es Auto, ej. Nissan Versa 2023)")
                                    if st.form_submit_button("Guardar Clasificación"):
                                        with engine.begin() as conn:
                                            conn.execute(text("UPDATE Polizas SET tipo_producto=:prod, vehiculo=:veh WHERE numero_poliza=:pol"), {"prod": nuevo_prod, "veh": nuevo_veh, "pol": poliza_clasificar})
                                        st.success("Etiquetas actualizadas"); st.rerun()
                    else: st.warning("Este cliente no tiene pólizas registradas.")
        else: st.info("No se encontró ningún cliente con esos datos.")

# ==========================================
# PESTAÑA 2: LECTOR IA MASIVO
# ==========================================
with pestana2:
    st.markdown("### 🧠 Motor de Extracción IA")
    st.write("1️⃣ **Selecciona a quién le pertenecen las pólizas que vas a subir:**")
    ejecutivo_seleccionado = st.selectbox("Asignar producción a:", lista_dinamica_ejecutivos)
    st.write("2️⃣ **Arrastra los PDFs (Pólizas y Recibos):**")
    
    st.caption("⚡ Modo Turbo (API de Pago Activada). El sistema procesará sin pausas artificiales.")
    
    archivos_subidos = st.file_uploader("Arrastra tus archivos aquí...", type=["pdf"], accept_multiple_files=True)
    
    if archivos_subidos and st.button("🚀 Iniciar Procesamiento Automático", type="primary"):
        total_archivos = len(archivos_subidos)
        st.info(f"Analizando {total_archivos} documento(s) para {ejecutivo_seleccionado}...")
        barra_progreso = st.progress(0)
        exitos = 0; errores = 0
        
        for i, archivo in enumerate(archivos_subidos):
            with st.spinner(f"Procesando: {archivo.name}... ({i+1}/{total_archivos})"):
                texto_crudo = extraer_texto_pdf(archivo)
                datos_json = None
                pdf_bytes = archivo.getvalue()
                error_api = ""
                
                if texto_crudo and len(texto_crudo.strip()) > 20: 
                    respuesta_texto = analizar_con_ia(texto_crudo)
                else:
                    ruta_temp = f"temp_{i}.pdf"
                    with open(ruta_temp, "wb") as f: f.write(pdf_bytes)
                    try:
                        for intento_vision in range(3):
                            try:
                                archivo_gemini = client.files.upload(file=ruta_temp)
                                instruccion_vision = f"Extrae informacion SOLO en JSON:\n{PLANTILLA_IA}"
                                response = client.models.generate_content(model='gemini-2.5-flash', contents=[archivo_gemini, instruccion_vision])
                                respuesta_texto = response.text
                                break
                            except Exception as ev:
                                error_str_v = str(ev)
                                if intento_vision < 2:
                                    time.sleep(2) # Pausa mínima solo si hay error de red
                                    continue
                                respuesta_texto = f"ERROR_API: {error_str_v}"
                                break
                    except Exception as e:
                        respuesta_texto = f"ERROR_API: {str(e)}"
                    finally:
                        if os.path.exists(ruta_temp): 
                            try: os.remove(ruta_temp)
                            except: pass
                
                if respuesta_texto and not respuesta_texto.startswith("ERROR_API"):
                    datos_json = limpiar_json(respuesta_texto)
                    if not datos_json: error_api = f"La IA no devolvió JSON válido."
                else:
                    error_api = respuesta_texto
                
                if datos_json:
                    resultado = guardar_poliza_bd(datos_json, pdf_bytes=pdf_bytes, ejecutivo=ejecutivo_seleccionado)
                    if isinstance(resultado, str) and not resultado.startswith("Error"): 
                        exitos += 1
                    else: 
                        errores += 1
                        st.error(f"🛑 Error de base de datos con {archivo.name}: {resultado}")
                else: 
                    errores += 1
                    st.error(f"⚠️ {archivo.name} no se pudo leer. Detalles: {error_api}")
            
            barra_progreso.progress((i + 1) / total_archivos)
            
            # ELIMINAMOS EL TIME.SLEEP DE AQUÍ PORQUE YA TIENES LLAVE DE PAGO
                
        if errores == 0:
            st.success(f"✅ ¡Se procesaron {exitos} documentos con éxito!")
            st.balloons()
        else: 
            st.warning(f"⚠️ {exitos} guardados exitosamente. Hubo {errores} errores.")

# ==========================================
# PESTAÑA 3: PROSPECTOS MANUALES
# ==========================================
with pestana3:
    col1, col2 = st.columns([1, 2])
    with col1:
        st.markdown("### 📝 Registrar Cotización")
        with st.form("form_prospectos", clear_on_submit=True):
            nombre = st.text_input("Nombre del prospecto")
            correo = st.text_input("Correo electrónico")
            telefono = st.text_input("Teléfono")
            producto = st.selectbox("Ramo cotizado", ["Autos", "Gastos Médicos Mayores", "Vida", "Daños Empresariales", "Hogar"])
            fecha_cotizacion = st.date_input("Fecha de cotización")
            ejecutivo_prospecto = st.selectbox("Ejecutivo / Sub-agente a cargo", lista_dinamica_ejecutivos)
            if st.form_submit_button("Guardar Prospecto", type="primary"):
                if nombre and telefono:
                    with engine.begin() as conn:
                        conn.execute(text("INSERT INTO Prospectos (nombre, correo, telefono, producto, fecha_cotizacion, ejecutivo) VALUES (:nom, :cor, :tel, :prod, :fec, :ejec)"),
                                    {"nom": nombre, "cor": correo, "tel": telefono, "prod": producto, "fec": fecha_cotizacion.strftime("%Y-%m-%d"), "ejec": ejecutivo_prospecto})
                    st.success("¡Guardado!"); st.rerun()
                else: st.error("Ingresa nombre y teléfono.")
    with col2:
        st.markdown("### 🚦 Embudo de Ventas")
        df_prospectos = pd.read_sql_query("SELECT nombre, telefono, producto, fecha_cotizacion, ejecutivo FROM Prospectos", engine)
        if not df_prospectos.empty:
            df_prospectos['fecha_cotizacion'] = pd.to_datetime(df_prospectos['fecha_cotizacion'])
            df_prospectos['Días'] = (pd.to_datetime(datetime.now().date()) - df_prospectos['fecha_cotizacion']).dt.days
            def aplicar_semaforo(valor):
                if valor <= 5: return 'background-color: #d4edda; color: #155724; font-weight: bold;'
                elif valor <= 10: return 'background-color: #fff3cd; color: #856404; font-weight: bold;'
                else: return 'background-color: #f8d7da; color: #721c24; font-weight: bold;'
            tabla_coloreada = df_prospectos.style.map(aplicar_semaforo, subset=['Días'])
            df_prospectos['fecha_cotizacion'] = df_prospectos['fecha_cotizacion'].dt.strftime('%d/%m/%Y')
            st.dataframe(tabla_coloreada, use_container_width=True, hide_index=True)

# ==========================================
# PESTAÑA 4: ALERTAS, COBRANZA Y CUMPLEAÑOS
# ==========================================
with pestana4:
    st.markdown("### 🔄 Próximas Renovaciones (Alerta 30 días)")
        
    df_alertas = pd.read_sql_query("SELECT c.nombre, c.telefono, p.aseguradora, p.numero_poliza, p.fin_vigencia, p.ejecutivo FROM Polizas p JOIN Clientes c ON p.rfc_cliente = c.rfc", engine)
    if not df_alertas.empty:
        df_alertas['fin_vigencia_dt'] = pd.to_datetime(df_alertas['fin_vigencia'], format='%d/%m/%Y', errors='coerce')
        df_alertas['Días Restantes'] = (df_alertas['fin_vigencia_dt'] - pd.to_datetime(datetime.now().date())).dt.days
        vencimientos = df_alertas[(df_alertas['Días Restantes'] <= 30) & (df_alertas['Días Restantes'] >= -5)].copy()
        if not vencimientos.empty:
            vencimientos['Aviso Renovación'] = [f"https://wa.me/52{str(tel).replace(' ','').replace('-','')}?text={urllib.parse.quote('Hola, te recuerdo que tu póliza vence pronto. ¿Te ayudo a renovarla?')}" for tel in vencimientos['telefono']]
            st.dataframe(vencimientos[['nombre', 'aseguradora', 'fin_vigencia', 'ejecutivo', 'Días Restantes', 'Aviso Renovación']], column_config={"Aviso Renovación": st.column_config.LinkColumn("💬 Enviar WhatsApp")}, hide_index=True, use_container_width=True)
        else: st.success("Todo tranquilo. No hay renovaciones urgentes.")
    
    st.markdown("---")
    st.markdown("### 💰 Control de Cobranza (Recibos Fraccionados)")
        
    df_cobranza = pd.read_sql_query("SELECT r.id, c.nombre, c.telefono, p.aseguradora, r.numero_poliza, r.monto, r.fecha_limite, p.ejecutivo FROM Recibos r JOIN Polizas p ON r.numero_poliza = p.numero_poliza JOIN Clientes c ON p.rfc_cliente = c.rfc WHERE r.estado = 'Pendiente'", engine)
    if not df_cobranza.empty:
        df_cobranza['fecha_dt'] = pd.to_datetime(df_cobranza['fecha_limite'], format='%d/%m/%Y', errors='coerce')
        df_cobranza['Dias_Atraso'] = (pd.to_datetime(datetime.now().date()) - df_cobranza['fecha_dt']).dt.days
        df_cobranza['monto'] = df_cobranza['monto'].apply(formato_pesos)
        
        estados = []; mensajes_wa = []
        for index, fila in df_cobranza.iterrows():
            dias = fila['Dias_Atraso']
            tel = str(fila['telefono']).replace(' ','').replace('-','')
            if dias <= 0: estados.append("🟢 A tiempo"); msj = f"Hola {fila['nombre']}, te recuerdo que el pago de tu póliza {fila['numero_poliza']} de {fila['aseguradora']} por {fila['monto']} vence el {fila['fecha_limite']}."
            elif 1 <= dias <= 15: estados.append("🟡 Rehabilitar (Gracia)"); msj = f"URGENTE: Hola {fila['nombre']}, tu recibo de {fila['aseguradora']} venció hace {dias} días. Aún estamos a tiempo de rehabilitar tu póliza."
            else: estados.append("🔴 Cancelada"); msj = f"Hola {fila['nombre']}, tu póliza de {fila['aseguradora']} ha sido cancelada por falta de pago."
            mensajes_wa.append(f"https://wa.me/52{tel}?text={urllib.parse.quote(msj)}")
            
        df_cobranza['Estatus'] = estados; df_cobranza['Aviso'] = mensajes_wa
        st.dataframe(df_cobranza[['nombre', 'aseguradora', 'monto', 'fecha_limite', 'ejecutivo', 'Estatus', 'Aviso']], column_config={"Aviso": st.column_config.LinkColumn("💬 Reclamar Pago")}, hide_index=True, use_container_width=True)
        
        with st.form("form_pagos"):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                opciones = df_cobranza.apply(lambda x: f"ID {x['id']} - {x['nombre']} - Póliza: {x['numero_poliza']} - {x['monto']}", axis=1).tolist()
                recibo_sel = st.selectbox("Selecciona el recibo que el cliente ya liquidó:", opciones)
            with col_b:
                st.write(""); st.write("")
                if st.form_submit_button("💰 Registrar Pago", type="primary"):
                    id_recibo = recibo_sel.split(" ")[1]
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE Recibos SET estado = 'Pagado' WHERE id = :id"), {"id": id_recibo})
                    st.success("¡El pago se ha registrado exitosamente!"); st.rerun()
    else: st.success("¡Felicidades! Tienes cartera sana, no hay recibos pendientes de cobro manual.")
        
    st.markdown("---")
    st.markdown("### 🎂 Cumpleañeros del Mes")
        
    df_cumples = pd.read_sql_query("SELECT nombre, telefono, fecha_nacimiento FROM Clientes WHERE fecha_nacimiento IS NOT NULL AND fecha_nacimiento != 'No especificado' AND fecha_nacimiento != 'No calculado'", engine)
    if not df_cumples.empty:
        df_cumples['fecha_dt'] = pd.to_datetime(df_cumples['fecha_nacimiento'], format='%d/%m/%Y', errors='coerce')
        valid_dates = df_cumples.dropna(subset=['fecha_dt']).copy()
        if not valid_dates.empty:
            mes_actual = datetime.now().month
            cumpleañeros = valid_dates[valid_dates['fecha_dt'].dt.month == mes_actual].copy()
            if not cumpleañeros.empty:
                cumpleañeros['Día'] = cumpleañeros['fecha_dt'].dt.day
                cumpleañeros = cumpleañeros.sort_values('Día')
                mensajes_cumple = []
                for index, fila in cumpleañeros.iterrows():
                    tel = str(fila['telefono']).replace(' ','').replace('-','')
                    msj = f"¡Hola {fila['nombre']}! 🎉 De parte de todo nuestro equipo, te deseamos un muy Feliz Cumpleaños. Que pases un excelente día lleno de alegría."
                    mensajes_cumple.append(f"https://wa.me/52{tel}?text={urllib.parse.quote(msj)}")
                
                cumpleañeros['Felicitar'] = mensajes_cumple
                st.dataframe(cumpleañeros[['nombre', 'fecha_nacimiento', 'telefono', 'Felicitar']], column_config={"Felicitar": st.column_config.LinkColumn("🎁 Enviar Felicitación")}, hide_index=True, use_container_width=True)
            else: st.success("No hay clientes que cumplan años en este mes.")
        else: st.info("Las fechas de nacimiento registradas no tienen el formato correcto (DD/MM/AAAA).")
    else: st.info("Aún no hay fechas de nacimiento registradas en el sistema.")

# ==========================================
# PESTAÑA 5: REPORTES VIP Y COMISIONES
# ==========================================
with pestana5:
    st.markdown("### 📊 Generador de Reportes Gerenciales y Comisiones")
    col_f1, col_f2 = st.columns(2)
    with col_f1: rango_fechas = st.date_input("🗓️ Filtra el periodo de análisis:", value=(datetime.now().date().replace(day=1), datetime.now().date()), format="DD/MM/YYYY")
    with col_f2:
        opciones_filtro = ["Todos los Ejecutivos"] + lista_dinamica_ejecutivos
        filtro_ejecutivo = st.selectbox("👤 Filtrar por Sub-agente (Comisiones):", opciones_filtro)
        
    if len(rango_fechas) == 2:
        fecha_inicio, fecha_fin = rango_fechas
        st.success(f"Mostrando datos del **{fecha_inicio.strftime('%d/%m/%Y')}** al **{fecha_fin.strftime('%d/%m/%Y')}** para **{filtro_ejecutivo}**")
        col_r1, col_r2, col_r3 = st.columns(3)
        
        with col_r1:
            st.info("📈 **Ventas (Nuevas Pólizas)**")
            df_ventas = pd.read_sql_query('SELECT c.nombre AS cliente, p.aseguradora AS aseguradora, p.numero_poliza AS poliza, p.inicio_vigencia AS inicio, p.ejecutivo AS ejecutivo FROM Polizas p JOIN Clientes c ON p.rfc_cliente = c.rfc', engine)
            if not df_ventas.empty:
                df_ventas.columns = ['cliente', 'aseguradora', 'poliza', 'inicio', 'ejecutivo']
                df_ventas['fecha_dt'] = pd.to_datetime(df_ventas['inicio'], format='%d/%m/%Y', errors='coerce')
                df_ventas_filtrado = df_ventas.loc[(df_ventas['fecha_dt'].dt.date >= fecha_inicio) & (df_ventas['fecha_dt'].dt.date <= fecha_fin)].drop(columns=['fecha_dt'])
                if filtro_ejecutivo != "Todos los Ejecutivos": df_ventas_filtrado = df_ventas_filtrado[df_ventas_filtrado['ejecutivo'] == filtro_ejecutivo]
                df_ventas_filtrado.columns = ['Cliente', 'Aseguradora', 'Poliza', 'Inicio', 'Ejecutivo']
                
                if not df_ventas_filtrado.empty:
                    st.download_button("📥 Excel (.csv)", data=df_ventas_filtrado.to_csv(index=False).encode('utf-8-sig'), file_name=f"Ventas_{filtro_ejecutivo}.csv", mime='text/csv', key='v_csv', use_container_width=True)
                    st.download_button("📄 PDF Oficial", data=generar_pdf_con_logos(df_ventas_filtrado, f"Ventas Nuevas - {filtro_ejecutivo}", fecha_inicio, fecha_fin), file_name=f"Ventas_{filtro_ejecutivo}.pdf", mime='application/pdf', key='v_pdf', use_container_width=True)
                else: st.warning("Sin datos para este filtro.")
            else: st.warning("Sin pólizas.")
            
        with col_r2:
            st.info("💰 **Historial de Cobranza**")
            df_cob = pd.read_sql_query('SELECT c.nombre AS cliente, p.aseguradora AS aseguradora, r.monto AS monto, r.fecha_limite AS limite, r.estado AS estatus, p.ejecutivo AS ejecutivo FROM Recibos r JOIN Polizas p ON r.numero_poliza = p.numero_poliza JOIN Clientes c ON p.rfc_cliente = c.rfc', engine)
            if not df_cob.empty:
                df_cob.columns = ['cliente', 'aseguradora', 'monto', 'limite', 'estatus', 'ejecutivo']
                df_cob['fecha_dt'] = pd.to_datetime(df_cob['limite'], format='%d/%m/%Y', errors='coerce')
                df_cob_filtrado = df_cob.loc[(df_cob['fecha_dt'].dt.date >= fecha_inicio) & (df_cob['fecha_dt'].dt.date <= fecha_fin)].drop(columns=['fecha_dt'])
                if filtro_ejecutivo != "Todos los Ejecutivos": df_cob_filtrado = df_cob_filtrado[df_cob_filtrado['ejecutivo'] == filtro_ejecutivo]
                df_cob_filtrado.columns = ['Cliente', 'Aseguradora', 'Monto', 'Limite', 'Estatus', 'Ejecutivo']
                df_cob_filtrado['Monto'] = df_cob_filtrado['Monto'].apply(formato_pesos)
                
                if not df_cob_filtrado.empty:
                    st.download_button("📥 Excel (.csv)", data=df_cob_filtrado.to_csv(index=False).encode('utf-8-sig'), file_name=f"Cobranza_{filtro_ejecutivo}.csv", mime='text/csv', key='c_csv', use_container_width=True)
                    st.download_button("📄 PDF Oficial", data=generar_pdf_con_logos(df_cob_filtrado, f"Cobranza - {filtro_ejecutivo}", fecha_inicio, fecha_fin), file_name=f"Cobranza_{filtro_ejecutivo}.pdf", mime='application/pdf', key='c_pdf', use_container_width=True)
                else: st.warning("Sin datos para este filtro.")
            else: st.warning("Sin recibos.")
            
        with col_r3:
            st.info("🎯 **Efectividad Prospectos**")
            df_prosp = pd.read_sql_query('SELECT nombre AS prospecto, producto AS producto, fecha_cotizacion AS fecha, ejecutivo AS ejecutivo FROM Prospectos', engine)
            if not df_prosp.empty:
                df_prosp.columns = ['prospecto', 'producto', 'fecha', 'ejecutivo']
                df_prosp['fecha_dt'] = pd.to_datetime(df_prosp['fecha'], format='%Y-%m-%d', errors='coerce')
                df_prosp_filtrado = df_prosp.loc[(df_prosp['fecha_dt'].dt.date >= fecha_inicio) & (df_prosp['fecha_dt'].dt.date <= fecha_fin)].drop(columns=['fecha_dt'])
                if filtro_ejecutivo != "Todos los Ejecutivos": df_prosp_filtrado = df_prosp_filtrado[df_prosp_filtrado['ejecutivo'] == filtro_ejecutivo]
                df_prosp_filtrado.columns = ['Prospecto', 'Producto', 'Fecha', 'Ejecutivo']
                
                if not df_prosp_filtrado.empty:
                    st.download_button("📥 Excel (.csv)", data=df_prosp_filtrado.to_csv(index=False).encode('utf-8-sig'), file_name=f"Prospectos_{filtro_ejecutivo}.csv", mime='text/csv', key='p_csv', use_container_width=True)
                    st.download_button("📄 PDF Oficial", data=generar_pdf_con_logos(df_prosp_filtrado, f"Prospectos - {filtro_ejecutivo}", fecha_inicio, fecha_fin), file_name=f"Prospectos_{filtro_ejecutivo}.pdf", mime='application/pdf', key='p_pdf', use_container_width=True)
                else: st.warning("Sin datos para este filtro.")
            else: st.warning("Sin prospectos.")
    else: st.info("Selecciona fechas en el calendario.")

# ==========================================
# FOOTER: FIRMA DEL CREADOR (POWERED BY)
# ==========================================
st.markdown("<br><br><br>", unsafe_allow_html=True)
st.markdown("---")
col_izq, col_centro, col_der = st.columns([4, 2, 4])

with col_centro:
    st.markdown("<p style='text-align: center; color: #888888; font-size: 13px; margin-bottom: 5px;'>Powered by:</p>", unsafe_allow_html=True)
    if os.path.exists("logo_creador.png"):
        st.image("logo_creador.png", use_container_width=True)
    else:
        st.markdown("<h4 style='text-align: center; color: #555555;'>URCO Lab</h4>", unsafe_allow_html=True)
