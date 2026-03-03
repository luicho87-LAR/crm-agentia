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

# --- 1. CONFIGURACIÓN DE INTELIGENCIA ARTIFICIAL Y PÁGINA ---
st.set_page_config(page_title="Agentia CRM", layout="wide", page_icon="icono_agentia.png")

# 🚨 ¡PEGA TU LLAVE AQUÍ ADENTRO DE LAS COMILLAS! 🚨
API_KEY = st.secrets["GEMINI_API_KEY"]
client = genai.Client(api_key=API_KEY)

# --- ✨ INYECCIÓN DE DISEÑO PREMIUM (UI/UX) ✨ ---
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

def analizar_con_ia(texto_sucio):
    instruccion = """Eres un experto en seguros. 1. Identifica "tipo_documento" ("Poliza" o "Recibo"). 2. Extrae: aseguradora, numero_poliza, nombre_cliente, rfc_cliente, telefono, correo, inicio_vigencia, fin_vigencia, direccion_completa. 3. Identifica "tipo_producto" (Autos, Gastos Médicos Mayores, Vida, Daños Empresariales, Hogar, u Otro). 4. Extrae en "vehiculo" (Marca, Modelo y Año si es auto, si no "N/A"). 5. Extrae cobranza: fecha_limite_pago, monto_a_pagar, y "forma_pago" (ej. Efectivo, Transferencia, Visa, Mastercard, Tarjeta de Credito, Amex, etc.). 6. Calcula "fecha_nacimiento" (DD/MM/AAAA) desde el RFC. Devuelve SOLO JSON válido sin markdown."""
    try:
        prompt_completo = f"{instruccion}\n\n--- DOCUMENTO ---\n{texto_sucio}"
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt_completo)
        
        texto_limpio = response.text
        # Limpieza segura para evitar errores de parseo markdown
        marcador_json = "```" + "json"
        marcador_fin = "```"
        if marcador_json in texto_limpio:
            texto_limpio = texto_limpio.split(marcador_json)[1].split(marcador_fin)[0]
        elif marcador_fin in texto_limpio:
            texto_limpio = texto_limpio.split(marcador_fin)[1].split(marcador_fin)[0]
            
        return json.loads(texto_limpio.strip())
    except Exception as e: 
        return None

def guardar_poliza_bd(datos, pdf_bytes=None, ejecutivo="Titular (Agencia)"):
    with engine.begin() as conn:
        try:
            tipo_doc = datos.get('tipo_documento', 'Poliza')
            
            # Salvavidas para RFC
            rfc = str(datos.get('rfc_cliente', '')).strip()
            if not rfc or rfc.lower() in ['no especificado', 'none', 'null']: 
                rfc = f"SIN_RFC_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                
            # Salvavidas para Póliza
            num_pol = str(datos.get('numero_poliza', '')).strip()
            if not num_pol or num_pol.lower() in ['no especificado', 'none', 'null', '']:
                num_pol = f"POR_ASIGNAR_{datetime.now().strftime('%H%M%S')}"
            
            conn.execute(text("""
                INSERT INTO Clientes (rfc, nombre, telefono, correo, fecha_nacimiento, direccion) 
                VALUES (:rfc, :nom, :tel, :cor, :fec, :dir) 
                ON CONFLICT (rfc) DO UPDATE SET 
                nombre=EXCLUDED.nombre, telefono=EXCLUDED.telefono, correo=EXCLUDED.correo, direccion=EXCLUDED.direccion
            """), {"rfc": rfc, "nom": datos.get('nombre_cliente'), "tel": datos.get('telefono'), "cor": datos.get('correo'), "fec": datos.get('fecha_nacimiento', 'No especificado'), "dir": datos.get('direccion_completa', 'No especificada')})
            
            prod = datos.get('tipo_producto', 'No especificado')
            veh = datos.get('vehiculo', 'N/A')
            
            if tipo_doc == 'Poliza':
                conn.execute(text("""
                    INSERT INTO Polizas (numero_poliza, rfc_cliente, aseguradora, inicio_vigencia, fin_vigencia, archivo_pdf, ejecutivo, tipo_producto, vehiculo) 
                    VALUES (:pol, :rfc, :aseg, :ini, :fin, :pdf, :ejec, :prod, :veh)
                    ON CONFLICT (numero_poliza) DO UPDATE SET 
                    inicio_vigencia=EXCLUDED.inicio_vigencia, fin_vigencia=EXCLUDED.fin_vigencia, archivo_pdf=EXCLUDED.archivo_pdf, ejecutivo=EXCLUDED.ejecutivo, tipo_producto=EXCLUDED.tipo_producto, vehiculo=EXCLUDED.vehiculo
                """), {"pol": num_pol, "rfc": rfc, "aseg": datos.get('aseguradora'), "ini": datos.get('inicio_vigencia'), "fin": datos.get('fin_vigencia'), "pdf": pdf_bytes, "ejec": ejecutivo, "prod": prod, "veh": veh})
            else:
                conn.execute(text("""
                    INSERT INTO Polizas (numero_poliza, rfc_cliente, aseguradora, inicio_vigencia, fin_vigencia, archivo_pdf, ejecutivo, tipo_producto, vehiculo) 
                    VALUES (:pol, :rfc, :aseg, :ini, :fin, :pdf, :ejec, :prod, :veh)
                    ON CONFLICT (numero_poliza) DO NOTHING
                """), {"pol": num_pol, "rfc": rfc, "aseg": datos.get('aseguradora'), "ini": datos.get('inicio_vigencia'), "fin": datos.get('fin_vigencia'), "pdf": pdf_bytes, "ejec": ejecutivo, "prod": prod, "veh": veh})
            
            fecha_pago = datos.get('fecha_limite_pago')
            if fecha_pago and str(fecha_pago).lower() not in ['no especificado', 'none', '']:
                monto = formato_pesos(datos.get('monto_a_pagar', 'No especificado'))
                forma_pago = str(datos.get('forma_pago', '')).lower()
                
                # Inteligencia de Tarjetas (CORREGIDO LA SINTAXIS "in")
                tarjetas_clave = ['visa', 'master', 'amex', 'tarjeta', 'credito', 'debito', 'cargo']
                if any(palabra in forma_pago for palabra in tarjetas_clave):
                    estado_recibo = 'Cargo Automático'
                else:
                    estado_recibo = 'Pendiente'
                
                res = conn.execute(text("SELECT id FROM Recibos WHERE numero_poliza=:pol AND fecha_limite=:fec"), {"pol": num_pol, "fec": fecha_pago}).fetchone()
                if not res:
                    conn.execute(text("INSERT INTO Recibos (numero_poliza, fecha_limite, monto, estado) VALUES (:pol, :fec, :mon, :est)"), {"pol": num_pol, "fec": fecha_pago, "mon": monto, "est": estado_recibo})
            return tipo_doc
        except Exception as e:
            return False

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

# --- 4. DISEÑO DE LA PANTALLA WEB ---
col_tit, col_vacia, col_der = st.columns([5, 1, 2])
with col_tit:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<h1 style='color: #0b7af0; margin-bottom: 0px;'>Sistema de Gestión Integral</h1>", unsafe_allow_html=True)
    st.caption("Inteligencia para vender más")

with col_der:
    if os.path.exists("logo_crm.png"):
        st.image("logo_crm.png", use_container_width=True)
    with st.popover("⚙️ Configuración del Equipo", use_container_width=True):
        st.markdown("#### 👤 Dar de alta a un nuevo Ejecutivo")
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
        st.markdown("---")
        st.markdown("#### 📋 Directorio Actual")
        df_equipo = pd.read_sql_query("SELECT id as ID, nombre as Nombre FROM Ejecutivos ORDER BY id", engine)
        st.dataframe(df_equipo, hide_index=True, use_container_width=True)

st.markdown("---")
lista_dinamica_ejecutivos = obtener_lista_ejecutivos()
pestana1, pestana2, pestana3, pestana4, pestana5, pestana6, pestana7 = st.tabs([
    "🔍 Buscador Inteligente", "📄 Lector IA Masivo", "🚦 Seguimiento Prospectos", 
    "🔔 Alertas y Cobranza", "📊 Reportes", "📥 Importador", "☁️ Respaldo a Drive"
])

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
                    df_polizas = pd.read_sql_query(f'SELECT aseguradora as "Aseguradora", numero_poliza as "Poliza", tipo_producto as "Ramo", vehiculo as "Vehículo", inicio_vigencia as "Inicio", fin_vigencia as "Fin", ejecutivo as "Ejecutivo" FROM Polizas WHERE rfc_cliente = \'{cliente["rfc"]}\'', engine)
                    if not df_polizas.empty:
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
    archivos_subidos = st.file_uploader("Arrastra tus archivos aquí...", type=["pdf"], accept_multiple_files=True)
    
    if archivos_subidos and st.button("🚀 Iniciar Procesamiento Automático", type="primary"):
        total_archivos = len(archivos_subidos)
        st.info(f"Analizando {total_archivos} documento(s) para {ejecutivo_seleccionado}...")
        barra_progreso = st.progress(0)
        exitos = 0; errores = 0
        
        for i, archivo in enumerate(archivos_subidos):
            with st.spinner(f"Leyendo: {archivo.name}... ({i+1}/{total_archivos})"):
                texto_crudo = extraer_texto_pdf(archivo)
                datos_json = None
                pdf_bytes = archivo.getvalue()
                
                if texto_crudo and len(texto_crudo.strip()) > 20: 
                    datos_json = analizar_con_ia(texto_crudo)
                else:
                    ruta_temp = f"temp_{i}.pdf"
                    with open(ruta_temp, "wb") as f: f.write(pdf_bytes)
                    try:
                        archivo_gemini = client.files.upload(file=ruta_temp)
                        instruccion_vision = """Eres experto en seguros. 1. Identifica "tipo_documento" ("Poliza" o "Recibo"). 2. Extrae: aseguradora, numero_poliza, nombre_cliente, rfc_cliente, telefono, correo, inicio_vigencia, fin_vigencia, direccion_completa. 3. Identifica "tipo_producto" (Autos, Gastos Médicos, Vida, Daños, Hogar, Otro). 4. Extrae "vehiculo" (Marca, Modelo y Año). 5. Extrae cobranza: fecha_limite_pago, monto_a_pagar, y "forma_pago" (Visa, Mastercard, Credito, Debito, etc.). Calcula fecha_nacimiento (DD/MM/AAAA). Devuelve SOLO JSON válido."""
                        response = client.models.generate_content(model='gemini-2.5-flash', contents=[archivo_gemini, instruccion_vision])
                        
                        texto_limpio = response.text
                        marcador_json = "```" + "json"
                        marcador_fin = "```"
                        if marcador_json in texto_limpio:
                            texto_limpio = texto_limpio.split(marcador_json)[1].split(marcador_fin)[0]
                        elif marcador_fin in texto_limpio:
                            texto_limpio = texto_limpio.split(marcador_fin)[1].split(marcador_fin)[0]
                            
                        datos_json = json.loads(texto_limpio.strip())
                    except: datos_json = None
                    if os.path.exists(ruta_temp): 
                        try: os.remove(ruta_temp)
                        except: pass
                
                if datos_json:
                    resultado = guardar_poliza_bd(datos_json, pdf_bytes=pdf_bytes, ejecutivo=ejecutivo_seleccionado)
                    if resultado: exitos += 1
                    else: errores += 1
                else: errores += 1
            barra_progreso.progress((i + 1) / total_archivos)
            
        if errores == 0:
            st.success(f"✅ ¡Se procesaron y automatizaron {exitos} documentos con éxito!")
            st.balloons()
        else: st.warning(f"⚠️ {exitos} guardados exitosamente. Hubo {errores} archivos ilegibles por falta de datos clave.")

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
            df_ventas = pd.read_sql_query('SELECT c.nombre as "Cliente", p.aseguradora as "Aseguradora", p.numero_poliza as "Poliza", p.inicio_vigencia as "Inicio", p.ejecutivo as "Ejecutivo" FROM Polizas p JOIN Clientes c ON p.rfc_cliente = c.rfc', engine)
            if not df_ventas.empty:
                df_ventas['fecha_dt'] = pd.to_datetime(df_ventas['Inicio'], format='%d/%m/%Y', errors='coerce')
                df_ventas_filtrado = df_ventas.loc[(df_ventas['fecha_dt'].dt.date >= fecha_inicio) & (df_ventas['fecha_dt'].dt.date <= fecha_fin)].drop(columns=['fecha_dt'])
                if filtro_ejecutivo != "Todos los Ejecutivos": df_ventas_filtrado = df_ventas_filtrado[df_ventas_filtrado['Ejecutivo'] == filtro_ejecutivo]
                if not df_ventas_filtrado.empty:
                    st.download_button("📥 Excel (.csv)", data=df_ventas_filtrado.to_csv(index=False).encode('utf-8-sig'), file_name=f"Ventas_{filtro_ejecutivo}.csv", mime='text/csv', key='v_csv', use_container_width=True)
                    st.download_button("📄 PDF Oficial", data=generar_pdf_con_logos(df_ventas_filtrado, f"Ventas Nuevas - {filtro_ejecutivo}", fecha_inicio, fecha_fin), file_name=f"Ventas_{filtro_ejecutivo}.pdf", mime='application/pdf', key='v_pdf', use_container_width=True)
                else: st.warning("Sin datos para este filtro.")
            else: st.warning("Sin pólizas.")
            
        with col_r2:
            st.info("💰 **Historial de Cobranza**")
            df_cob = pd.read_sql_query('SELECT c.nombre as "Cliente", p.aseguradora as "Aseguradora", r.monto as "Monto", r.fecha_limite as "Limite", r.estado as "Estatus", p.ejecutivo as "Ejecutivo" FROM Recibos r JOIN Polizas p ON r.numero_poliza = p.numero_poliza JOIN Clientes c ON p.rfc_cliente = c.rfc', engine)
            if not df_cob.empty:
                df_cob['fecha_dt'] = pd.to_datetime(df_cob['Limite'], format='%d/%m/%Y', errors='coerce')
                df_cob_filtrado = df_cob.loc[(df_cob['fecha_dt'].dt.date >= fecha_inicio) & (df_cob['fecha_dt'].dt.date <= fecha_fin)].drop(columns=['fecha_dt'])
                if filtro_ejecutivo != "Todos los Ejecutivos": df_cob_filtrado = df_cob_filtrado[df_cob_filtrado['Ejecutivo'] == filtro_ejecutivo]
                if 'Monto' in df_cob_filtrado.columns: df_cob_filtrado['Monto'] = df_cob_filtrado['Monto'].apply(formato_pesos)
                if not df_cob_filtrado.empty:
                    st.download_button("📥 Excel (.csv)", data=df_cob_filtrado.to_csv(index=False).encode('utf-8-sig'), file_name=f"Cobranza_{filtro_ejecutivo}.csv", mime='text/csv', key='c_csv', use_container_width=True)
                    st.download_button("📄 PDF Oficial", data=generar_pdf_con_logos(df_cob_filtrado, f"Cobranza - {filtro_ejecutivo}", fecha_inicio, fecha_fin), file_name=f"Cobranza_{filtro_ejecutivo}.pdf", mime='application/pdf', key='c_pdf', use_container_width=True)
                else: st.warning("Sin datos para este filtro.")
            else: st.warning("Sin recibos.")
            
        with col_r3:
            st.info("🎯 **Efectividad Prospectos**")
            df_prosp = pd.read_sql_query('SELECT nombre as "Prospecto", producto as "Producto", fecha_cotizacion as "Fecha", ejecutivo as "Ejecutivo" FROM Prospectos', engine)
            if not df_prosp.empty:
                df_prosp['fecha_dt'] = pd.to_datetime(df_prosp['Fecha'], format='%Y-%m-%d', errors='coerce')
                df_prosp_filtrado = df_prosp.loc[(df_prosp['fecha_dt'].dt.date >= fecha_inicio) & (df_prosp['fecha_dt'].dt.date <= fecha_fin)].drop(columns=['fecha_dt'])
                if filtro_ejecutivo != "Todos los Ejecutivos": df_prosp_filtrado = df_prosp_filtrado[df_prosp_filtrado['Ejecutivo'] == filtro_ejecutivo]
                if not df_prosp_filtrado.empty:
                    st.download_button("📥 Excel (.csv)", data=df_prosp_filtrado.to_csv(index=False).encode('utf-8-sig'), file_name=f"Prospectos_{filtro_ejecutivo}.csv", mime='text/csv', key='p_csv', use_container_width=True)
                    st.download_button("📄 PDF Oficial", data=generar_pdf_con_logos(df_prosp_filtrado, f"Prospectos - {filtro_ejecutivo}", fecha_inicio, fecha_fin), file_name=f"Prospectos_{filtro_ejecutivo}.pdf", mime='application/pdf', key='p_pdf', use_container_width=True)
                else: st.warning("Sin datos para este filtro.")
            else: st.warning("Sin prospectos.")
    else: st.info("Selecciona fechas en el calendario.")

# ==========================================
# PESTAÑA 6: IMPORTADOR MASIVO
# ==========================================
with pestana6:
    st.markdown("### 📥 Migración de Cartera (Excel/CSV)")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("##### 1. Descarga el formato base")
        df_plantilla = pd.DataFrame(columns=['Nombre_Completo', 'RFC', 'Telefono', 'Correo', 'Direccion', 'Aseguradora', 'Numero_Poliza', 'Inicio_Vigencia_DD/MM/AAAA', 'Fin_Vigencia_DD/MM/AAAA', 'Ejecutivo'])
        st.download_button(label="📥 Descargar Plantilla.csv", data=df_plantilla.to_csv(index=False).encode('utf-8-sig'), file_name="Plantilla_Agentia.csv", mime='text/csv', type="primary")
        
    with col_b:
        st.markdown("##### 2. Sube tus datos")
        archivo_importar = st.file_uploader("Sube tu archivo lleno (.csv o .xlsx)", type=["csv", "xlsx"])
        if archivo_importar and st.button("🚀 Iniciar Carga a la Base de Datos", type="primary"):
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
                            
                            conn.execute(text("""
                                INSERT INTO Clientes (rfc, nombre, telefono, correo, fecha_nacimiento, direccion) 
                                VALUES (:rfc, :nom, :tel, :cor, 'No calculado', :dir)
                                ON CONFLICT (rfc) DO UPDATE SET nombre=EXCLUDED.nombre, telefono=EXCLUDED.telefono, correo=EXCLUDED.correo, direccion=EXCLUDED.direccion
                            """), {"rfc": rfc, "nom": nombre, "tel": tel, "cor": correo, "dir": direc})
                            
                            if pol and pol != 'nan':
                                conn.execute(text("""
                                    INSERT INTO Polizas (numero_poliza, rfc_cliente, aseguradora, inicio_vigencia, fin_vigencia, ejecutivo, tipo_producto, vehiculo) 
                                    VALUES (:pol, :rfc, :aseg, :ini, :fin, :ejec, 'No especificado', 'N/A')
                                    ON CONFLICT (numero_poliza) DO UPDATE SET inicio_vigencia=EXCLUDED.inicio_vigencia, fin_vigencia=EXCLUDED.fin_vigencia, ejecutivo=EXCLUDED.ejecutivo
                                """), {"pol": pol, "rfc": rfc, "aseg": aseg, "ini": ini, "fin": fin, "ejec": ejecutivo_excel})
                            registros += 1
                    st.success(f"🎉 ¡Misión cumplida! Se migraron {registros} clientes a tu nube."); st.balloons()
                except Exception as e: st.error(f"Error técnico en el archivo: {e}")

# ==========================================
# PESTAÑA 7: EXPORTAR A DRIVE DEL CLIENTE
# ==========================================
with pestana7:
    st.markdown("### ☁️ Respaldo Local (Para Google Drive)")
    st.info("Tus datos viven seguros en la nube de Supabase. Sin embargo, si deseas tener una copia física en tu computadora para subirla a tu **Google Drive personal**, utiliza este botón. Se generará un archivo ZIP con todos tus registros en formato Excel (.csv).")
    
    if st.button("📦 Generar y Descargar Respaldo Total", type="primary"):
        with st.spinner("Empaquetando toda la base de datos..."):
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
                
                st.success("¡Respaldo generado con éxito!")
                st.download_button(
                    label="📥 Guardar Archivo ZIP en mi Computadora",
                    data=zip_buffer.getvalue(),
                    file_name=f"Respaldo_Agentia_{datetime.now().strftime('%Y%m%d')}.zip",
                    mime="application/zip",
                    type="secondary"
                )
            except Exception as e:
                st.error(f"Error al generar el respaldo: {e}")

# ==========================================
# FOOTER: FIRMA DEL CREADOR (POWERED BY)
# ==========================================
st.markdown("<br><br><br>", unsafe_allow_html=True)
st.markdown("---")
col_izq, col_centro, col_der = st.columns([4, 2, 4])

with col_centro:
    st.markdown("<p style='text-align: center; color: #888888; font-size: 13px; margin-bottom: 5px;'>Powered by:</p>", unsafe_allow_html=True)
    
    # Cambia "logo_creador.png" por el nombre real de tu logo en GitHub
    if os.path.exists("logo_creador.png"):
        st.image("logo_creador.png", use_container_width=True)
    else:
        st.markdown("<h4 style='text-align: center; color: #555555;'>URCO Lab</h4>", unsafe_allow_html=True)

