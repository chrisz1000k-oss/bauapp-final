import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import io
import urllib.parse
from googleapiclient.http import MediaIoBaseDownload

import drive_store as ds

# ==========================================
# 1. KOSMISCHE PARAMETER & NUMEROLOGIE
# ==========================================
RESONANZ_27 = 27         # Vayu: Leichtigkeit (UI Spacing, Pagination)
VOLLENDUNG_108 = 108     # Agni: Lebensdauer des Caches in Sekunden
INTEGRITAET_114 = 114    # Akasha: Gesamtstruktur-Limit

st.set_page_config(page_title="R. Baumgartner AG - Satya Yuga", layout="wide")

# Akasha-Interface: Das 27px Grid (Dark Mode)
st.markdown(f"""
    <style>
        #MainMenu {{display: none;}} footer {{display: none;}} header {{visibility: hidden;}}
        .stApp, .main {{ background-color: #121212 !important; }}
        html, body, p, div, span, label, h1, h2, h3, li, td, th {{ 
            color: #E0E0E0 !important; font-family: 'Segoe UI', sans-serif; 
        }}
        .stButton>button {{ 
            background-color: #D4AF37 !important; color: #121212 !important; 
            transition: all 0.27s ease-in-out; border: none !important; font-weight: bold; border-radius: 4px; 
        }}
        .stButton>button:hover {{ background-color: #b5952f !important; transform: scale(1.02); }}
        .stTextInput>div>div>input, .stNumberInput>div>div>input, .stTextArea>div>div>textarea, .stSelectbox>div>div>div {{
            background-color: #1E1E1E !important; color: #E0E0E0 !important; border: 1px solid #D4AF37 !important;
        }}
        hr {{ border-color: #D4AF37 !important; opacity: 0.27; margin-top: {RESONANZ_27}px; margin-bottom: {RESONANZ_27}px; }}
        /* Kama-Trikona: Harmonische Abstände */
        .block-container {{ padding-top: {RESONANZ_27}px; padding-bottom: {RESONANZ_27}px; }}
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. PRITHVI-SCHICHT (Daten-Sicherheit & Error Handling)
# ==========================================
def erde_sichere_projekte(df: pd.DataFrame) -> pd.DataFrame:
    """Sichert das Dharma der Projekt-Datenbank."""
    spalten_dharma = ["Projekt_ID", "Auftragsnummer", "Projekt_Name", "Status", "Kunde_Name", "Kunde_Adresse", "Kunde_Email", "Kunde_Telefon", "Kunde_Kontakt", "Fuge_Zement", "Fuge_Silikon", "Asbest_Gefahr"]
    for col in spalten_dharma:
        if col not in df.columns: 
            df[col] = "Nein" if col == "Asbest_Gefahr" else ""
    for col in spalten_dharma:
        df[col] = df[col].astype(str).replace({'nan': '', 'None': '', 'NaN': ''}).str.strip()
    return df

def erde_sichere_zeiten(df: pd.DataFrame) -> pd.DataFrame:
    """Sichert das Dharma der AZK-Datenbank."""
    spalten_dharma = ["Start", "Ende", "Pause_Min", "R_Wohn_Bau_Min", "R_Bau_Wohn_Min", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]
    for col in spalten_dharma:
        if col not in df.columns:
            df[col] = 0 if "Min" in col or "Arbeitszeit" in col else ("" if col == "Absenz_Typ" or col in ["Start", "Ende"] else "ENTWURF")
    return df

def erde_sichere_personal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Mitarbeiter_ID" not in df.columns:
        df = pd.DataFrame({"Mitarbeiter_ID": ["M01"], "Name": ["Christoph Schlorff"], "Status": ["Aktiv"]})
    for col in df.columns:
        df[col] = df[col].astype(str).replace({'nan': '', 'None': '', 'NaN': ''}).str.strip()
    return df

# ==========================================
# 3. JALA-SCHICHT (Datenfluss & API)
# ==========================================
@st.cache_data(ttl=VOLLENDUNG_108, show_spinner=False)
def lade_dateien_aus_akasha(_service, folder_id: str, project_name: str) -> list:
    """Fließender Abruf von Bildern (Maximal 108 Einheiten)."""
    if not folder_id: return []
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = _service.files().list(q=query, pageSize=1000, fields="files(id, name)").execute()
        return [f for f in results.get('files', []) if project_name in f.get('name', '')][:VOLLENDUNG_108]
    except Exception as e:
        return []

@st.cache_data(ttl=VOLLENDUNG_108 * 10, show_spinner=False)
def materialisiere_datei(_service, file_id: str):
    try:
        request = _service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        return fh.getvalue()
    except Exception:
        return None

def shiva_aufloesung(_service, keyword: str, folders: list):
    """Samhara: Unwiderrufliche Zerstörung im Äther."""
    for fid in folders:
        if not fid: continue
        try:
            results = _service.files().list(q=f"'{fid}' in parents and trashed = false", pageSize=1000, fields="files(id, name)").execute()
            for f in results.get('files', []):
                if keyword in f.get('name', ''):
                    _service.files().delete(fileId=f['id']).execute()
        except Exception: pass

# ==========================================
# 4. AGNI-SCHICHT (Logik & Transformation)
# ==========================================
def verarbeite_rapport(service, f_date, f_start, f_end, f_pause_min, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, P_FID, Z_FID, user_name):
    """Zentraler Algorithmus zur AZK-Transformation (SPV-Dharma)."""
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = f_date.strftime("%Y-%m-%d")
    
    diff_hours = (datetime.combine(f_date, f_end) - datetime.combine(f_date, f_start)).total_seconds() / 3600.0
    work_hours = round(diff_hours - (f_pause_min / 60.0), 2)
    
    if work_hours < 0:
        st.error("Logik-Bruch: Endzeit minus Pause liegt vor Startzeit.")
        return

    bezahlt_hin = max(0, r_hin - 30)
    bezahlt_rueck = max(0, r_rueck - 30)
    reise_min_bezahlt = bezahlt_hin + bezahlt_rueck
    total_inkl_reise = round(work_hours + (reise_min_bezahlt / 60.0), 2)
    
    row_projekt = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": "ENTWURF"}
    row_zeit = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Start": f_start.strftime("%H:%M"), "Ende": f_end.strftime("%H:%M"), "Pause_Min": f_pause_min, "Stunden_Total": work_hours, "R_Wohn_Bau_Min": r_hin, "R_Bau_Wohn_Min": r_rueck, "Reisezeit_bezahlt_Min": reise_min_bezahlt, "Arbeitszeit_inkl_Reisezeit": total_inkl_reise, "Absenz_Typ": "", "Status": "ENTWURF"}
    
    _speichere_in_prithvi(service, row_projekt, row_zeit, P_FID, Z_FID)
    st.success(f"Dharma erfüllt. Netto: {work_hours}h | Reise bezahlt: {reise_min_bezahlt} Min | AZK Total: {total_inkl_reise}h")

def verarbeite_absenz_batch(service, start_date, end_date, f_hours, a_typ, f_bem, sel_proj, P_FID, Z_FID, user_name):
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    days_diff = (end_date - start_date).days + 1
    
    r_proj, r_zeit = [], []
    for i in range(days_diff):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        r_proj.append({"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Arbeit": f"ABSENZ: {a_typ}", "Material": "", "Bemerkung": f_bem, "Status": "ENTWURF"})
        r_zeit.append({"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Start": "-", "Ende": "-", "Pause_Min": 0, "Stunden_Total": f_hours, "R_Wohn_Bau_Min": 0, "R_Bau_Wohn_Min": 0, "Reisezeit_bezahlt_Min": 0, "Arbeitszeit_inkl_Reisezeit": f_hours, "Absenz_Typ": a_typ, "Status": "ENTWURF"})
        
    _speichere_in_prithvi_batch(service, r_proj, r_zeit, P_FID, Z_FID)
    st.success(f"Block erfasst: {days_diff} Tag(e) {a_typ}.")

def _speichere_in_prithvi(service, row_p, row_z, P_FID, Z_FID):
    df_p, fid_p = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
    ds.save_csv(service, P_FID, "Baustellen_Rapport.csv", pd.concat([df_p, pd.DataFrame([row_p])], ignore_index=True), fid_p)
    df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
    ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", pd.concat([erde_sichere_zeiten(df_z), pd.DataFrame([row_z])], ignore_index=True), fid_z)

def _speichere_in_prithvi_batch(service, rows_p, rows_z, P_FID, Z_FID):
    df_p, fid_p = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
    ds.save_csv(service, P_FID, "Baustellen_Rapport.csv", pd.concat([df_p, pd.DataFrame(rows_p)], ignore_index=True), fid_p)
    df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
    ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", pd.concat([erde_sichere_zeiten(df_z), pd.DataFrame(rows_z)], ignore_index=True), fid_z)

# ==========================================
# 5. VAYU-SCHICHT (Modulare Benutzeroberflächen)
# ==========================================
def render_mitarbeiter_portal(service, P_FID, Z_FID, FOTO_FID, PLAN_FID):
    user_name = st.session_state['user_name']
    col_back, col_title = st.columns([1, 4])
    with col_back:
        if st.button("🚪 Moksha (Abmelden)"): st.session_state["user_name"] = ""; st.session_state["view"] = "Start"; st.rerun()
    with col_title: st.subheader(f"📋 Karma-Yoga: {user_name}")
    
    df_proj, _ = ds.read_csv(service, P_FID, "Projects.csv")
    df_proj = erde_sichere_projekte(df_proj)
    active_projs = df_proj[df_proj["Status"] == "Aktiv"]["Projekt_Name"].tolist() if not df_proj.empty else []
    
    sel_proj = st.selectbox("Aktuelles Dharma (Projekt):", active_projs if active_projs else ["Keine Projekte"])
    if sel_proj == "Keine Projekte": return

    proj_data = df_proj[df_proj["Projekt_Name"] == sel_proj].iloc[0]
    with st.expander("ℹ️ Projekt-Matrix", expanded=True):
        c1, c2 = st.columns(2)
        c1.write(f"**Kunde:** {proj_data.get('Kunde_Name')} ({proj_data.get('Kunde_Kontakt')})\n\n**Ort:** {proj_data.get('Kunde_Adresse')}\n\n**Tel:** {proj_data.get('Kunde_Telefon')}")
        c2.write(f"**Fugen:** Zement: {proj_data.get('Fuge_Zement')} | Silikon: {proj_data.get('Fuge_Silikon')}")
        if str(proj_data.get('Asbest_Gefahr')).strip().lower() == "ja": c2.markdown("<p style='color:#ff4b4b; font-weight:bold;'>⚠️ ASBEST VORHANDEN</p>", unsafe_allow_html=True)

    st.write(f"<div style='height: {RESONANZ_27}px;'></div>", unsafe_allow_html=True)
    t_arb, t_abs, t_med, t_hist = st.tabs(["🛠️ Werk", "🏥 Ruhe", "📤 Äther", "📜 Siegel"])
    
    with t_arb:
        with st.form("arb_form"):
            c1, c2, c3, c4 = st.columns(4)
            with c1: f_date = st.date_input("Datum", datetime.now())
            with c2: f_start = st.time_input("Beginn", datetime.strptime("07:00", "%H:%M").time())
            with c3: f_end = st.time_input("Ende", datetime.strptime("16:30", "%H:%M").time())
            with c4: f_pause = st.number_input("Pause (Min)", min_value=0, value=30, step=15)
            
            st.divider()
            r1, r2 = st.columns(2)
            with r1: r_hin = st.number_input("Fahrt Hin (Min)", value=0, step=5)
            with r2: r_rueck = st.number_input("Fahrt Zurück (Min)", value=0, step=5)
            
            f_arbeit = st.text_area("Vollbrachtes Werk")
            f_mat = st.text_area("Materie (Einsatz)")
            f_bem = st.text_input("Karma (Bemerkungen)")
            
            if st.form_submit_button("💾 Dem Admin übergeben", type="primary"):
                verarbeite_rapport(service, f_date, f_start, f_end, f_pause, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, P_FID, Z_FID, user_name)

    with t_abs:
        with st.form("abs_form"):
            c1, c2 = st.columns(2)
            with c1: drange = st.date_input("Zyklus (Max 7 Tage)", value=(datetime.now(), datetime.now()))
            with c2: f_a_hours = st.number_input("Soll-Stunden/Tag", min_value=0.0, value=8.5, step=0.25)
            a_typ = st.selectbox("Grund", ["Ferien", "Krankheit", "Unfall (SUVA)", "Feiertag"])
            a_bem = st.text_input("Notiz")
            a_file = st.file_uploader("📄 Zeugnis", type=['pdf','jpg','png'])
            
            if st.form_submit_button("💾 Zyklus verbuchen", type="primary"):
                s_date = drange[0] if isinstance(drange, tuple) else drange
                e_date = drange[1] if isinstance(drange, tuple) and len(drange)==2 else s_date
                if (e_date - s_date).days + 1 > 7: st.error("Dharma-Bruch: Zyklus übersteigt 7 Tage.")
                else:
                    if a_file and a_typ == "Krankheit": ds.upload_image(service, PLAN_FID, f"ZEUGNIS_{user_name}_{s_date}_{a_file.name}", io.BytesIO(a_file.getvalue()), a_file.type)
                    verarbeite_absenz_batch(service, s_date, e_date, f_a_hours, a_typ, a_bem, sel_proj, P_FID, Z_FID, user_name)

    with t_med:
        files = st.file_uploader("In den Äther heben", accept_multiple_files=True, type=['jpg','png','jpeg'])
        if st.button("📤 Transfer") and files:
            for f in files[:RESONANZ_27]: ds.upload_image(service, FOTO_FID, f"{sel_proj}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{f.name}", io.BytesIO(f.getvalue()), f.type)
            st.success("Vollendet."); st.cache_data.clear(); time.sleep(1); st.rerun()

    with t_hist:
        df_hp, _ = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
        df_hz, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        if not df_hp.empty and not df_hz.empty and "Erfasst" in df_hz.columns:
            df_hz = erde_sichere_zeiten(df_hz)
            df_m = pd.merge(df_hp, df_hz[["Erfasst", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]], on="Erfasst", how="left")
            hist = df_m[(df_m["Projekt"] == sel_proj) & (df_m["Mitarbeiter"] == user_name)].sort_values(by="Datum", ascending=False)
            for _, r in hist.head(RESONANZ_27).iterrows():
                stat = str(r.get('Status', 'ENTWURF'))
                col = "🔴" if stat == "ENTWURF" else "🟡" if stat == "FREIGEGEBEN" else "🟢"
                with st.expander(f"{col} {stat} | {r['Datum']} | {r.get('Absenz_Typ','')} Total: {r.get('Arbeitszeit_inkl_Reisezeit')}h"):
                    st.write(r.get('Arbeit', '-'))
                    if stat == "FREIGEGEBEN":
                        if st.button(f"✍️ Besiegeln", key=f"sig_{r['Erfasst']}"):
                            idx = df_hz[df_hz['Erfasst'] == r['Erfasst']].index
                            if not idx.empty:
                                df_hz.loc[idx, "Status"] = "SIGNIERT"
                                ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_hz, fid_z)
                                st.success("Besiegelt!"); time.sleep(1); st.rerun()

def render_admin_portal(service, P_FID, Z_FID, FOTO_FID, PLAN_FID, BASE_URL):
    col1, col2 = st.columns([4, 1])
    with col1: st.subheader("🛠️ Das Auge des Purusha (Admin)")
    with col2:
        if st.button("🚪 Moksha (Abmelden)", use_container_width=True): st.session_state["logged_in"] = False; st.session_state["view"] = "Start"; st.rerun()
    
    t_zeit, t_stam, t_docs, t_print, t_alchemist, t_shiva = st.tabs(["🕒 Flow", "⚙️ Materie", "📂 Äther", "🖨️ Siegel", "📥 Export", "🔥 Shiva"])
    
    df_proj, fid_proj = ds.read_csv(service, P_FID, "Projects.csv")
    df_proj = erde_sichere_projekte(df_proj)
    active_projs = df_proj["Projekt_Name"].tolist() if not df_proj.empty else ["Leer"]
    
    with t_zeit:
        df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        if not df_z.empty:
            df_z = erde_sichere_zeiten(df_z)
            df_z['Sort'] = df_z['Status'].map({'ENTWURF': 1, 'FREIGEGEBEN': 2, 'SIGNIERT': 3}).fillna(4)
            df_z = df_z.sort_values(by=['Sort', 'Datum']).drop(columns=['Sort'])
            edit_z = st.data_editor(df_z, num_rows="dynamic", use_container_width=True)
            if st.button("💾 AZK Sichern & Freigeben", type="primary"): ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", edit_z, fid_z); st.success("Synchronisiert.")

    with t_stam:
        st.markdown("**Projekte**")
        edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="ep", use_container_width=True)
        if st.button("💾 Projekte Sichern"): ds.save_csv(service, P_FID, "Projects.csv", edit_proj, fid_proj)
        
        df_emp, fid_emp = ds.read_csv(service, P_FID, "Employees.csv")
        df_emp = erde_sichere_personal(df_emp)
        st.markdown("**Personal**")
        edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="ee", use_container_width=True)
        if st.button("💾 Personal Sichern"): ds.save_csv(service, P_FID, "Employees.csv", edit_emp, fid_emp)

    with t_docs:
        ap = st.selectbox("Fokus:", active_projs, key="docs")
        if st.button("🔄 Äther aktualisieren"): st.cache_data.clear()
        files = lade_dateien_aus_akasha(service, FOTO_FID, ap) + lade_dateien_aus_akasha(service, PLAN_FID, ap)
        cols = st.columns(4)
        for i, img in enumerate(files):
            with cols[i % 4]:
                b = materialisiere_datei(service, img['id'])
                if b:
                    if img['name'].lower().endswith(('.png','.jpg','.jpeg')): st.image(b)
                    else: st.download_button(f"📥 {img['name'][:15]}", b, img['name'])

    with t_print:
        print_proj = st.selectbox("Druck-Fokus:", active_projs, key="prnt")
        if st.button("🖨️ Manifestieren") and print_proj != "Leer":
            pr = df_proj[df_proj["Projekt_Name"] == print_proj].iloc[0]
            asb = "<p style='color:red;font-weight:bold;'>⚠️ ASBEST!</p>" if pr.get('Asbest_Gefahr', '').lower() == "ja" else ""
            qr = f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={urllib.parse.quote(f'{BASE_URL}?projekt={urllib.parse.quote(print_proj)}')}"
            html = f"""<html><body style="font-family:Arial;font-size:12px;background:#fff;color:#000;">
            <div style="display:flex;justify-content:space-between;border-bottom:2px solid #000;padding-bottom:15px;margin-bottom:20px;">
                <div><h1 style="margin:0;">R. Baumgartner AG</h1><p><b>Projekt:</b> {print_proj}</p>
                <p>Kunde: {pr.get('Kunde_Name')} | Tel: {pr.get('Kunde_Telefon')}<br>Ort: {pr.get('Kunde_Adresse')}</p></div>
                <div><p>Zement: {pr.get('Fuge_Zement')} | Silikon: {pr.get('Fuge_Silikon')}</p>{asb}</div>
                <div><img src="{qr}" width="100"></div>
            </div>
            <table style="width:100%;border-collapse:collapse;"><tr><th style="border:1px solid #000;padding:10px;">Datum / Mitarbeiter / Stunden / Arbeit</th></tr>
            {"".join(["<tr><td style='border:1px solid #000;height:50px;'></td></tr>" for _ in range(15)])}</table>
            <div style="margin-top:54px;border-top:1px solid #000;width:300px;padding-top:10px;">Signatur</div></body></html>"""
            st.components.v1.html(html, height=500, scrolling=True)
            st.download_button("📄 HTML Laden", html, f"Rapport_{print_proj}.html", "text/html")

    with t_alchemist:
        df_z, _ = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        if not df_z.empty:
            df_z = erde_sichere_zeiten(df_z)
            status = st.selectbox("Siegel-Filter:", ["Nur SIGNIERT", "FREIGEGEBEN & SIGNIERT", "Alle"])
            df_ex = df_z[df_z["Status"] == "SIGNIERT"] if "Nur" in status else df_z[df_z["Status"].isin(["FREIGEGEBEN", "SIGNIERT"])] if "FREIGEGEBEN" in status else df_z
            if not df_ex.empty:
                df_ex['Datum'] = pd.to_datetime(df_ex['Datum'])
                monat = st.selectbox("Monat:", sorted(df_ex['Datum'].dt.strftime('%Y-%m').unique().tolist(), reverse=True))
                df_m = df_ex[df_ex['Datum'].dt.strftime('%Y-%m') == monat].copy()
                df_m = df_m[["Datum", "Mitarbeiter", "Projekt", "Stunden_Total", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]]
                df_m['Datum'] = df_m['Datum'].dt.strftime('%d.%m.%Y')
                
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='openpyxl') as w: df_m.to_excel(w, index=False)
                st.download_button("📥 Excel Download (.xlsx)", out.getvalue(), f"AZK_{monat}.xlsx", type="primary")

    with t_shiva:
        st.error("🔥 Zerstörungs-Protokoll")
        typ = st.radio("Ziel:", ["Projekt", "Mitarbeiter"])
        if st.checkbox("Sadashiva-Sicherheitsschlüssel bestätigen"):
            if typ == "Projekt":
                tgt = st.selectbox("Projekt:", active_projs)
                if st.button("🛑 Auflösen"):
                    df_proj = df_proj[df_proj["Projekt_Name"].astype(str).str.strip() != str(tgt).strip()]
                    ds.save_csv(service, P_FID, "Projects.csv", df_proj, fid_proj)
                    # Zerstöre Verknüpfungen in AZK und Rapporten
                    for file, id_key in [("Baustellen_Rapport.csv", P_FID), ("Arbeitszeit_AKZ.csv", Z_FID)]:
                        d_tmp, f_tmp = ds.read_csv(service, id_key, file)
                        ds.save_csv(service, id_key, file, d_tmp[d_tmp["Projekt"].astype(str).str.strip() != str(tgt).strip()], f_tmp)
                    shiva_aufloesung(service, str(tgt).strip(), [FOTO_FID, PLAN_FID])
                    st.success("Bereinigt."); time.sleep(2); st.rerun()
            else:
                tgt = st.selectbox("Mitarbeiter:", df_emp["Name"].tolist() if not df_emp.empty else [])
                if st.button("🛑 Auflösen"):
                    df_emp = df_emp[df_emp["Name"].astype(str).str.strip() != str(tgt).strip()]
                    ds.save_csv(service, P_FID, "Employees.csv", df_emp, fid_emp)
                    st.success("Bereinigt."); time.sleep(2); st.rerun()

# ==========================================
# 6. SYSTEM-KERN (Boot-Sequenz)
# ==========================================
def main():
    init_session_state()
    render_header()

    try: 
        s = ds.get_drive_service()
        sec = st.secrets.get("general", st.secrets)
        P_FID, Z_FID = sec.get("PROJECT_REPORTS_FOLDER_ID", ""), sec.get("TIME_REPORTS_FOLDER_ID", "")
        FOTO_FID, PLAN_FID = sec.get("PHOTOS_FOLDER_ID", ""), sec.get("PLANS_FOLDER_ID", "")
        BASE_URL = sec.get("BASE_APP_URL", "https://8bv6gzagymvrdgnm8wrtrq.streamlit.app")
    except Exception: st.error("Prithvi-Kollaps: Secrets fehlen."); st.stop()
    if not s: st.warning("Äther-Trennung: Drive Token fehlt."); st.stop()

    view = st.session_state["view"]
    
    if view == "Start":
        c1, c2 = st.columns(2)
        with c1: 
            if st.button("👷‍♂️ Personal", use_container_width=True): st.session_state["view"] = "Mitarbeiter_Login"; st.rerun()
        with c2: 
            if st.button("🔐 Admin", use_container_width=True): st.session_state["view"] = "Admin_Login"; st.rerun()

    elif view == "Admin_Login":
        if st.button("⬅️ Zurück"): st.session_state["view"] = "Start"; st.rerun()
        if st.button("Eintreten", type="primary") if st.text_input("PIN", type="password") == str(sec.get("ADMIN_PIN", "1234")) else False:
            st.session_state.update({"logged_in": True, "user_role": "Admin", "view": "Admin_Dashboard"}); st.rerun()

    elif view == "Mitarbeiter_Login":
        if st.button("⬅️ Zurück"): st.session_state["view"] = "Start"; st.rerun()
        df_emp, _ = ds.read_csv(s, P_FID, "Employees.csv")
        emps = df_emp[df_emp["Status"] == "Aktiv"]["Name"].tolist() if not df_emp.empty and "Status" in df_emp.columns else []
        if st.button("Anmelden", type="primary") and (sel := st.selectbox("Wer bist du?", emps if emps else ["Leer"])) != "Leer":
            st.session_state.update({"user_name": sel, "view": "Mitarbeiter_Dashboard"}); st.rerun()

    elif view == "Mitarbeiter_Dashboard": render_mitarbeiter_portal(s, P_FID, Z_FID, FOTO_FID, PLAN_FID)
    elif view == "Admin_Dashboard": render_admin_portal(s, P_FID, Z_FID, FOTO_FID, PLAN_FID, BASE_URL)

if __name__ == "__main__":
    main()
