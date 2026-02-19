import io
import os
import sqlite3
from datetime import date, datetime

import pytz

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from requests.auth import HTTPBasicAuth

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="FPM Dashboard",
    layout="wide",
    page_icon="🚜",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background-color: #F7F8F3;
        color: #1B4332;
    }

    .block-container {
        padding-top: 2.5rem;
        padding-bottom: 5rem;
        max-width: 100%;
    }

    /* Sync Bar */
    .sync-bar {
        display: flex;
        align-items: center;
        gap: 10px;
        font-size: 0.8rem;
        color: #52796F;
        margin-bottom: 8px;
    }
    .sync-bar .last-update {
        background: #E8EDE5;
        padding: 4px 12px;
        border-radius: 8px;
        font-weight: 500;
    }

    /* Cards */
    .grafico-card {
        background-color: #ffffff;
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
        border: 1px solid #E8EDE5;
        margin-bottom: 24px;
        transition: box-shadow 0.2s ease;
    }
    .grafico-card:hover {
        box-shadow: 0 4px 12px rgba(27,67,50,0.08);
    }

    /* KPI Cards */
    div[data-testid="metric-container"] {
        background-color: #ffffff;
        padding: 18px 24px;
        border-radius: 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        border: 1px solid #E8EDE5;
        border-left: 5px solid #2D6A4F;
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(45,106,79,0.10);
    }
    div[data-testid="metric-container"] label {
        color: #52796F !important;
        font-weight: 500 !important;
    }
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #1B4332 !important;
        font-weight: 700 !important;
    }

    .chart-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1B4332;
        margin-bottom: 15px;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .section-divider {
        margin: 28px 0;
        border: 0;
        border-top: 1px solid #D8E2DC;
    }

    .streamlit-expanderHeader {
        font-size: 0.85rem;
        color: #52796F;
        font-weight: 500;
    }

    /* Botão de Download */
    .stDownloadButton button {
        background-color: #2D6A4F !important;
        color: white !important;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 1.2rem;
        font-weight: 500;
        transition: background-color 0.2s ease, transform 0.1s ease;
    }
    .stDownloadButton button:hover {
        background-color: #1B4332 !important;
        transform: translateY(-1px);
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: #F0F4EF; border-radius: 4px; }
    ::-webkit-scrollbar-thumb { background: #95A89A; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #52796F; }
    </style>
""",
    unsafe_allow_html=True,
)

# --- 2. CONFIGURAÇÕES ---
DB_FILE = "dados_fazenda.db"
BASE_URL = st.secrets["api"]["base_url"]
CLIENTE = st.secrets["api"]["cliente"]
TOKEN = st.secrets["api"]["token"]
AUTH_USER = st.secrets["api"]["auth_user"]
AUTH_PASS = st.secrets["api"]["auth_pass"]

# Fuso horário padrão — Streamlit Cloud roda em UTC
FUSO_SP = pytz.timezone("America/Sao_Paulo")

FILIAIS_ALVO = ["2", "5", "1"]
ID_UNIDADE = "1"
TIPO_TICKET = "Entrada Produção"

PALAVRAS_PROIBIDAS = [
    "FILTRO",
    "OLEO",
    "ÓLEO",
    "PECA",
    "PEÇA",
    "PARAFUSO",
    "ARRUELA",
    "LUBRIFICANTE",
    "ADUB",
    "FERTILIZANTE",
    "SEMENTE",
    "DIESEL",
    "ZETHA",
    "HERBICIDA",
    "FUNGICIDA",
]


# --- 3. BACKEND ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("SELECT obs FROM analise_produtividade LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("DROP TABLE IF EXISTS analise_produtividade")
        conn.commit()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS analise_produtividade (
            id_ticket TEXT PRIMARY KEY, data TEXT, numero_romaneio TEXT,
            local_safra TEXT, safra_agricola TEXT, produto_full TEXT,
            cultura TEXT, variedade TEXT, divisor REAL, peso_bruto REAL,
            desconto REAL, peso_liquido REAL, sacas REAL, hectares REAL,
            id_local_estoque TEXT, obs TEXT
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            ultima_sync TEXT
        )
    """
    )
    conn.commit()
    conn.close()


def get_json(endpoint, d_ini=None, d_fim=None):
    if d_ini and d_fim:
        url = f"{BASE_URL}/{endpoint}/{d_ini}/{d_fim}/{CLIENTE}/{TOKEN}"
    else:
        url = f"{BASE_URL}/{endpoint}/{CLIENTE}/{TOKEN}"
    try:
        r = requests.get(url, auth=HTTPBasicAuth(AUTH_USER, AUTH_PASS))
        return r.json() if r.status_code == 200 else []
    except:
        return []


def sincronizar_dados(modo="parcial"):
    status = st.empty()
    bar = st.progress(0)
    hoje = datetime.now(FUSO_SP)

    if modo == "parcial":
        dt_inicio = date(hoje.year, hoje.month, 1)
        dt_fim = hoje.date()
    else:
        dt_inicio = date(2020, 1, 1)
        dt_fim = hoje.date()

    str_ini = dt_inicio.strftime("%d%m%Y")
    str_fim = dt_fim.strftime("%d%m%Y")

    status.info("Baixando Cadastros...")
    areas = get_json("areas")
    subareas = get_json("subareas")
    anos = get_json("anos")
    produtos = get_json("produtos")
    nomes_prod = get_json("produtosnomes")
    variedades = get_json("produtosvariedades")
    bar.progress(15)

    map_areas = {a["idArea"]: a["area"] for a in areas}
    map_sub = {
        s["idSubArea"]: {"nome": s["subArea"], "pai": s["idArea"]} for s in subareas
    }
    map_local_final = {}
    map_safra_ano = {}

    for a in anos:
        d_sub = map_sub.get(a.get("idSubArea"))
        ano_label = a.get("ano")
        map_safra_ano[a.get("idAno")] = ano_label
        if d_sub:
            fazenda = map_areas.get(d_sub["pai"], "Desc.")
            map_local_final[a.get("idAno")] = (
                f"{fazenda} - {d_sub['nome']} ({ano_label})"
            )
        else:
            map_local_final[a.get("idAno")] = f"ID {a.get('idAno')}"

    map_nomes = {
        p["idNomeProduto"]: p["nomeProduto"].upper()
        for p in nomes_prod
        if p.get("nomeProduto")
    }
    map_var = {
        v["idVariedade"]: v["nomeVariedade"].upper()
        for v in variedades
        if v.get("nomeVariedade")
    }
    map_prod_final = {}

    for p in produtos:
        # Filtro primário: apenas produtos do grupo 12 (Produtos Produzidos)
        if str(p.get("idGrupo")) != "12":
            continue

        n_base = map_nomes.get(p.get("idNomeProduto"), "DESC")
        n_var = map_var.get(p.get("idVariedade"), "")

        # Filtro secundário: palavras proibidas como dupla segurança
        eh_lixo = any(proibida in n_base for proibida in PALAVRAS_PROIBIDAS)

        if not eh_lixo:
            p_id = str(p.get("idProduto"))
            if n_var and n_var != "NH-NENHUM":
                nome_full = f"{n_base} ({n_var}) #{p_id}" 
                var_clean = n_var
            else:
                nome_full = f"{n_base} #{p_id}"
                var_clean = "COMUM"
            
            div = 50.0 if "BATATA" in n_base else 60.0
            map_prod_final[p_id] = {
                "nome_full": nome_full,
                "cultura": n_base,
                "variedade": var_clean,
                "divisor": div,
            }

    status.info("Baixando Movimentações...")
    tickets = get_json("ticketscompras", str_ini, str_fim)
    bar.progress(40)

    valid_tickets = {}
    for t in tickets:
        unid_fat = str(t.get("idUnidadeFaturamento") or "")
        eh_unidade_valida = (unid_fat == ID_UNIDADE) or (unid_fat in ["", "None", "0"])

        if (
            str(t.get("idFilial")) in FILIAIS_ALVO
            and eh_unidade_valida
            and TIPO_TICKET in str(t.get("tipoTicket", ""))
        ):

            obs_txt = t.get("observacao") or t.get("obs") or ""

            valid_tickets[t.get("idTicketCompra")] = {
                "numero": t.get("numeroTicket"),
                "data": t.get("dataTicket"),
                "obs": obs_txt,
            }

    itens = get_json("ticketscomprasitens", str_ini, str_fim)
    destinacoes = get_json("ticketscomprasdestinacoes", str_ini, str_fim)
    bar.progress(70)

    item_map = {}
    for i in itens:
        if i.get("idTicketCompra") in valid_tickets:
            item_map[i.get("idTicketCompraItem")] = {
                "tid": i.get("idTicketCompra"),
                "pid": str(i.get("idProduto")),
            }

    rows = []

    for d in destinacoes:
        iid = d.get("idTicketCompraItem")
        if iid in item_map:
            info_item = item_map[iid]
            pid = info_item["pid"]

            if pid not in map_prod_final:
                continue

            p_data = map_prod_final[pid]
            info_ticket = valid_tickets[info_item["tid"]]

            id_ano_origem = d.get("idAno") or d.get("safra")
            nome_local = map_local_final.get(id_ano_origem, "N/D")
            safra_lbl = map_safra_ano.get(id_ano_origem, "N/D")

            try:
                qtd = float(d.get("quantidade") or 0)
                desc = float(d.get("quantidadeDesconto") or 0)
                hec = float(str(d.get("hectare") or 0).replace(",", "."))
                liq = qtd - desc
                sacas = liq / p_data["divisor"]

                if sacas <= 0:
                    continue

                dt_sql = info_ticket["data"].split(" ")[0]

                rows.append(
                    {
                        "id_ticket": d.get("idTicketCompraDestinacao"),
                        "data": dt_sql,
                        "numero_romaneio": info_ticket["numero"],
                        "local_safra": nome_local,
                        "safra_agricola": safra_lbl,
                        "produto_full": p_data["nome_full"],
                        "cultura": p_data["cultura"],
                        "variedade": p_data["variedade"],
                        "divisor": p_data["divisor"],
                        "peso_bruto": qtd,
                        "desconto": desc,
                        "peso_liquido": liq,
                        "sacas": sacas,
                        "hectares": hec,
                        "id_local_estoque": str(d.get("idLocalEstoque")),
                        "obs": info_ticket["obs"],
                    }
                )
            except:
                continue

    conn = sqlite3.connect(DB_FILE)
    if modo == "total":
        if rows:
            df_new = pd.DataFrame(rows)
            df_new["data"] = pd.to_datetime(df_new["data"])
            df_new.to_sql(
                "analise_produtividade", conn, if_exists="replace", index=False
            )
    elif modo == "parcial":
        cursor = conn.cursor()
        sql_del = "DELETE FROM analise_produtividade WHERE data >= ? AND data <= ?"
        cursor.execute(
            sql_del, (dt_inicio.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d"))
        )
        conn.commit()
        if rows:
            df_new = pd.DataFrame(rows)
            df_new["data"] = pd.to_datetime(df_new["data"])
            df_new.to_sql(
                "analise_produtividade", conn, if_exists="append", index=False
            )
    conn.close()

    # Registra timestamp da sincronização no banco
    conn_log = sqlite3.connect(DB_FILE)
    conn_log.execute(
        "INSERT OR REPLACE INTO sync_log (id, ultima_sync) VALUES (1, ?)",
        (datetime.now(FUSO_SP).isoformat(),),
    )
    conn_log.commit()
    conn_log.close()

    bar.progress(100)
    status.success(f"Atualizado! {len(rows)} registros.")
    st.cache_data.clear()


init_db()


def precisa_sincronizar() -> bool:
    """Retorna True se a última sync é anterior às 06:00 de hoje (SP)."""
    try:
        conn = sqlite3.connect(DB_FILE)
        row = conn.execute(
            "SELECT ultima_sync FROM sync_log WHERE id = 1"
        ).fetchone()
        conn.close()
        if not row:
            return True  # Nunca sincronizou
        ultima = datetime.fromisoformat(row[0])
        if ultima.tzinfo is None:
            ultima = FUSO_SP.localize(ultima)
        limite = datetime.now(FUSO_SP).replace(
            hour=6, minute=0, second=0, microsecond=0
        )
        return ultima < limite
    except Exception:
        return True


@st.cache_data(ttl=300)
def ler_dados():
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql("SELECT * FROM analise_produtividade", conn)
        if df.empty or "safra_agricola" not in df.columns:
            return pd.DataFrame()

        df["data"] = pd.to_datetime(df["data"])
        df["safra_agricola"] = df["safra_agricola"].astype(str).replace("nan", "N/D")
        df["variedade"] = df["variedade"].astype(str)
        df["talhao_limpo"] = df["local_safra"].str.replace(
            r"\s\(\d{2,4}.*\)", "", regex=True
        )

        filtro_lixo = "|".join(PALAVRAS_PROIBIDAS)
        df = df[~df["produto_full"].str.contains(filtro_lixo, case=False, na=False)]
        df = df[df["sacas"] > 0]

        return df
    except:
        return pd.DataFrame()
    finally:
        conn.close()


def converter_df_para_excel(df):
    output = io.BytesIO()
    colunas_export = [
        "data",
        "numero_romaneio",
        "local_safra",
        "safra_agricola",
        "cultura",
        "variedade",
        "peso_bruto",
        "desconto",
        "peso_liquido",
        "sacas",
        "hectares",
        "obs",
    ]
    cols_existentes = [c for c in colunas_export if c in df.columns]
    df_export = df[cols_existentes].copy()
    if "data" in df_export.columns:
        df_export["data"] = df_export["data"].dt.strftime("%d/%m/%Y")
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Dados Detalhados")
        worksheet = writer.sheets["Dados Detalhados"]
        for i, col in enumerate(df_export.columns):
            max_len = max(df_export[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_len)
    return output.getvalue()


# --- 4. INTERFACE ---

# Gatilho 1: ?update=true na URL → força sync parcial + limpa parâmetro
_params = st.query_params
if _params.get("update") == "true":
    st.cache_data.clear()
    sincronizar_dados("parcial")
    del _params["update"]
    st.rerun()

# Gatilho 2: auto-sync se a última sync é anterior às 06:00 de hoje (SP)
if precisa_sincronizar():
    sincronizar_dados("parcial")
    st.rerun()

df_clean = ler_dados()

# ── SIDEBAR: Painel de Controle ──
with st.sidebar:
    st.markdown("## 🚜 Painel de Controle")
    st.caption("Use os controles abaixo para sincronizar dados e filtrar o dashboard.")

    # ── Sincronização ──
    st.markdown("---")
    st.markdown("#### 🔄 Sincronização")

    try:
        _db_mtime = os.path.getmtime(DB_FILE)
        _last_update = datetime.fromtimestamp(_db_mtime, tz=FUSO_SP).strftime("%d/%m/%Y %H:%M")
    except Exception:
        _last_update = "—"

    st.markdown(
        f'<div class="sync-bar">'
        f'<span>🕐</span>'
        f'<span class="last-update">Última atualização: {_last_update}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    _btn1, _btn2 = st.columns(2)
    with _btn1:
        if st.button("🔄 Atualizar Mês", use_container_width=True):
            sincronizar_dados("parcial")
            st.rerun()
    with _btn2:
        if st.button("⚠️ Atualizar Tudo", use_container_width=True):
            sincronizar_dados("total")
            st.rerun()

    if df_clean.empty:
        st.warning("⚠️ Banco vazio. Clique em Atualizar.")
        st.stop()

    # ── Filtros Hierárquicos (Cascata) ──
    st.markdown("---")
    st.markdown("#### 🔍 Filtros")
    st.caption("Os filtros são encadeados: cada seleção refina o próximo.")

    # 1. Filtro DATA (Mestre)
    d_min, d_max = df_clean["data"].min().date(), df_clean["data"].max().date()
    datas = st.slider("Período", d_min, d_max, (d_min, d_max), format="DD/MM/YYYY")
    df_1 = df_clean[
        (df_clean["data"].dt.date >= datas[0]) & (df_clean["data"].dt.date <= datas[1])
    ]

    # 2. Filtro CULTURA (Depende da Data)
    opcoes_cultura = sorted(df_1["cultura"].fillna("N/D").unique())
    sel_cultura = st.multiselect("Cultura", options=opcoes_cultura)
    df_2 = df_1[df_1["cultura"].isin(sel_cultura)] if sel_cultura else df_1

    # 3. Filtro SAFRA (Depende da Cultura)
    opcoes_safra = sorted(df_2["safra_agricola"].unique())
    sel_safra = st.multiselect("Safra Agrícola", options=opcoes_safra)
    df_3 = df_2[df_2["safra_agricola"].isin(sel_safra)] if sel_safra else df_2

    # 4. Filtro ÁREA/TALHÃO (Depende da Safra)
    opcoes_area = sorted(df_3["talhao_limpo"].unique())
    sel_area = st.multiselect("Área (Talhão)", options=opcoes_area)
    df_4 = df_3[df_3["talhao_limpo"].isin(sel_area)] if sel_area else df_3

    # 5. Filtro VARIEDADE (Depende da Área)
    opcoes_var = sorted(df_4["variedade"].unique())
    sel_variedade = st.multiselect("Variedade", options=opcoes_var)
    df_view = df_4[df_4["variedade"].isin(sel_variedade)] if sel_variedade else df_4

    # Detecta se algum filtro foi selecionado
    filtros_ativos = bool(sel_cultura or sel_safra or sel_area or sel_variedade)

    # ── Exportação ──
    st.markdown("---")
    if not df_view.empty:
        excel_data = converter_df_para_excel(df_view)
        st.download_button(
            label="📥 Baixar Excel",
            data=excel_data,
            file_name=f'relatorio_filtrado_{datetime.now(FUSO_SP).strftime("%d_%m_%Y")}.xlsx',
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

if df_view.empty:
    st.warning("🚜 Nenhum dado encontrado para esta combinação de filtros. Tente alterar a data ou a área.")
    st.stop()

# --- VISÃO PADRÃO: ROMANEIOS RECENTES (quando nenhum filtro selecionado) ---
if not filtros_ativos:
    st.markdown(
        '<div class="chart-header">📋 Romaneios Recentes</div>',
        unsafe_allow_html=True,
    )
    df_recentes = (
        df_view.sort_values("data", ascending=False)
        .head(50)
        .copy()
    )
    df_recentes["data"] = df_recentes["data"].dt.strftime("%d/%m/%Y")
    _df_rec_styled = df_recentes[
        [
            "data",
            "numero_romaneio",
            "talhao_limpo",
            "cultura",
            "variedade",
            "peso_liquido",
            "sacas",
        ]
    ].rename(columns={
        "data": "Data",
        "numero_romaneio": "Romaneio",
        "talhao_limpo": "Área",
        "cultura": "Cultura",
        "variedade": "Variedade",
        "peso_liquido": "Peso Líq. (kg)",
        "sacas": "Sacas",
    })
    try:
        # NOTA: background_gradient requer matplotlib (ver requirements.txt)
        _styled_rec = _df_rec_styled.style.format(
            {"Peso Líq. (kg)": "{:,.0f}", "Sacas": "{:,.1f}"}
        ).background_gradient(subset=["Sacas"], cmap="Greens")
    except ImportError:
        _styled_rec = _df_rec_styled.style.format(
            {"Peso Líq. (kg)": "{:,.0f}", "Sacas": "{:,.1f}"}
        )
    st.dataframe(_styled_rec, use_container_width=True, height=500)
    st.stop()

# --- KPI GERAL ---
try:
    df_kpi_area = (
        df_view.groupby(["safra_agricola", "local_safra", "cultura", "produto_full"])
        .agg({"hectares": "max"})
        .reset_index()
    )
    tot_ha = df_kpi_area["hectares"].sum()
    tot_sacas = df_view["sacas"].sum()
    prod_geral = tot_sacas / tot_ha if tot_ha > 0 else 0
    tot_liq_ton = df_view["peso_liquido"].sum() / 1000

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Produtividade", f"{prod_geral:.1f} sc/ha")
    k2.metric("Total Colhido", f"{tot_sacas:,.0f} sc")
    k3.metric("Área Colhida", f"{tot_ha:,.1f} ha")
    k4.metric("Volume Líquido", f"{tot_liq_ton:,.1f} ton")
except Exception:
    st.error("⚠️ Ocorreu um erro ao calcular os indicadores. Por favor, ajuste os filtros.")

st.markdown("<br>", unsafe_allow_html=True)

# === GRÁFICOS ===
try:
    c_g1, c_g2 = st.columns([2, 1])

    # PREPARA DADOS
    df_var_area = (
        df_view.groupby(["cultura", "variedade", "produto_full", "local_safra"])["hectares"]
        .max()
        .reset_index()
    )
    df_var_area_sum = (
        df_var_area.groupby(["cultura", "variedade"])["hectares"].sum().reset_index()
    )

    talhoes_validos = df_var_area["local_safra"].unique()
    df_sacas_validas = df_view[df_view["local_safra"].isin(talhoes_validos)]
    df_var_sacas_sum = (
        df_sacas_validas.groupby(["cultura", "variedade"])["sacas"].sum().reset_index()
    )

    df_rank = pd.merge(df_var_sacas_sum, df_var_area_sum, on=["cultura", "variedade"])
    df_rank["yield"] = df_rank["sacas"] / df_rank["hectares"]
    df_rank = df_rank.sort_values("yield", ascending=False)

    # GRÁFICO 1
    with c_g1:
        with st.container():
            st.markdown(
                '<div class="chart-header">🏆 Eficiência por Variedade (Média Real)</div>',
                unsafe_allow_html=True,
            )

            from plotly.subplots import make_subplots

            fig_rank = make_subplots(specs=[[{"secondary_y": True}]])

            fig_rank.add_trace(
                go.Bar(
                    x=df_rank["variedade"],
                    y=df_rank["yield"],
                    name="Sacas/ha",
                    marker_color="#2D6A4F",
                    text=df_rank["yield"].round(0).astype(int),
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>Produtividade: %{y:.1f} sc/ha<extra></extra>",
                ),
                secondary_y=False,
            )

            fig_rank.add_trace(
                go.Scatter(
                    x=df_rank["variedade"],
                    y=df_rank["hectares"],
                    name="Hectares",
                    mode="lines+markers+text",
                    line=dict(color="#DDA15E", width=3),
                    marker=dict(size=8, color="#DDA15E"),
                    text=df_rank["hectares"].round(1),
                    textposition="top center",
                    textfont=dict(color="#B07D3B", size=10),
                    hovertemplate="<b>%{x}</b><br>Área: %{y:.1f} ha<extra></extra>",
                ),
                secondary_y=True,
            )

            max_yield = df_rank["yield"].max() if not df_rank.empty else 100
            fig_rank.update_layout(
                margin=dict(l=0, r=0, t=35, b=0),
                height=420,
                showlegend=True,
                legend=dict(orientation="h", y=-0.25),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                clickmode="event+select",
                xaxis=dict(type="category"),
                bargap=0.3,
                hoverlabel=dict(
                    bgcolor="rgba(27, 67, 50, 0.92)",
                    font_size=13,
                    font_family="Inter",
                    font_color="#ffffff",
                    bordercolor="rgba(255,255,255,0.15)",
                ),
            )
            fig_rank.update_yaxes(
                title_text="Sacas / Hectare",
                secondary_y=False,
                range=[0, max_yield * 1.18],
            )
            fig_rank.update_yaxes(title_text="Hectares", secondary_y=True)

            selection = st.plotly_chart(
                fig_rank, use_container_width=True, on_select="rerun"
            )

    # INTERATIVIDADE
    df_interactive = df_view.copy()
    if selection and selection.get("selection") and selection["selection"].get("points"):
        selected_points = selection["selection"]["points"]
        selected_varieties = [p["x"] for p in selected_points]
        if selected_varieties:
            df_interactive = df_view[df_view["variedade"].isin(selected_varieties)]

    # GRÁFICO 2
    with c_g2:
        with st.container():
            st.markdown(
                '<div class="chart-header">📉 Qualidade (Perdas)</div>',
                unsafe_allow_html=True,
            )
            liq = df_interactive["peso_liquido"].sum()
            desc = df_interactive["desconto"].sum()

            fig_pie = go.Figure(
                data=[
                    go.Pie(
                        labels=["Líquido", "Quebra"],
                        values=[liq, desc],
                        hole=0.6,
                        marker_colors=["#2D6A4F", "#C1121F"],
                        textinfo="percent",
                        hovertemplate="<b>%{label}</b><br>%{value:,.0f} kg<br>%{percent}<extra></extra>",
                    )
                ]
            )
            fig_pie.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                height=350,
                showlegend=True,
                legend=dict(orientation="h", y=-0.2),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                hoverlabel=dict(
                    bgcolor="rgba(27, 67, 50, 0.92)",
                    font_size=13,
                    font_family="Inter",
                    font_color="#ffffff",
                    bordercolor="rgba(255,255,255,0.15)",
                ),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    # --- GRÁFICO DE EVOLUÇÃO ---
    st.markdown(
        '<div class="chart-header">📅 Evolução Diária (Toneladas por Talhão)</div>',
        unsafe_allow_html=True,
    )
    with st.container():
        df_evo_prep = df_interactive.copy()
        df_evo_prep["data_only"] = df_evo_prep["data"].dt.strftime("%d/%m/%Y")
        df_evo_prep["toneladas"] = df_evo_prep["peso_liquido"] / 1000

        df_evo = (
            df_evo_prep.groupby(["data_only", "talhao_limpo"])["toneladas"]
            .sum()
            .reset_index()
        )
        # Ordenar por data real
        df_evo["_sort"] = pd.to_datetime(df_evo["data_only"], format="%d/%m/%Y")
        df_evo = df_evo.sort_values("_sort").drop(columns="_sort")

        fig_evo = px.bar(
            df_evo,
            x="data_only",
            y="toneladas",
            color="talhao_limpo",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_evo.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=350,
            xaxis_title=None,
            yaxis_title="Toneladas",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            bargap=0.2,
            showlegend=True,
            legend=dict(orientation="h", y=-0.3),
            xaxis=dict(type="category"),
            hoverlabel=dict(
                bgcolor="rgba(27, 67, 50, 0.92)",
                font_size=13,
                font_family="Inter",
                font_color="#ffffff",
                bordercolor="rgba(255,255,255,0.15)",
            ),
        )
        fig_evo.update_traces(
            hovertemplate="<b>%{x}</b><br>%{fullData.name}<br>%{y:.2f} ton<extra></extra>"
        )
        st.plotly_chart(fig_evo, use_container_width=True)

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    # --- TABELAS ---
    st.markdown("### 📋 Detalhamento")

    df_tab_base = (
        df_interactive.groupby(["safra_agricola", "local_safra", "cultura", "variedade", "produto_full"])
        .agg({"hectares": "max", "sacas": "sum"})
        .reset_index()
    )

    df_tab_base["produtividade"] = df_tab_base["sacas"] / df_tab_base["hectares"]
    df_tab_base = df_tab_base[df_tab_base["produtividade"] <= 2000]

    df_tab_display = df_tab_base.sort_values("produtividade", ascending=False).copy()
    try:
        # NOTA: background_gradient requer matplotlib (ver requirements.txt)
        _styled_tab = df_tab_display.style.format(
            {"sacas": "{:,.1f}", "hectares": "{:,.2f}", "produtividade": "{:,.2f}"}
        ).background_gradient(subset=["produtividade"], cmap="Greens")
    except ImportError:
        _styled_tab = df_tab_display.style.format(
            {"sacas": "{:,.1f}", "hectares": "{:,.2f}", "produtividade": "{:,.2f}"}
        )
    st.dataframe(_styled_tab, use_container_width=True, height=400)

    # --- AUDITORIA ---
    with st.expander("🕵️ Auditoria de Maiores Cargas"):
        st.markdown(
            "Lista das 50 maiores cargas (útil para achar devoluções ou duplicidades)."
        )
        top_tickets = df_interactive.sort_values("sacas", ascending=False).head(50).copy()
        top_tickets["data"] = top_tickets["data"].dt.strftime("%d/%m/%Y")
        _df_audit = top_tickets[
            [
                "data",
                "numero_romaneio",
                "produto_full",
                "sacas",
                "hectares",
                "obs",
            ]
        ]
        try:
            # NOTA: background_gradient requer matplotlib (ver requirements.txt)
            _styled_audit = _df_audit.style.format(
                {"sacas": "{:,.1f}", "hectares": "{:,.2f}"}
            ).background_gradient(subset=["sacas"], cmap="Reds")
        except ImportError:
            _styled_audit = _df_audit.style.format(
                {"sacas": "{:,.1f}", "hectares": "{:,.2f}"}
            )
        st.dataframe(_styled_audit, use_container_width=True)

except Exception:
    st.error("⚠️ Ocorreu um erro ao processar esta visualização. Por favor, ajuste os filtros.")