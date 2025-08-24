import pandas as pd
import plotly.express as px
import os


class CovidTemporalAnalysis:
    def __init__(self,
                 input_csv: str = "dataset_covid.csv",
                 out_mensual_csv: str = "casos_mensuales_por_pais.csv",
                 out_ordenado_csv: str = "dataset_covid_ordenado.csv",
                 out_tendencias_csv: str = "analisis/tendencias_paises.csv",
                 pais_especifico: str = "El Salvador",
                 random_state: int = 42):
        self.input_csv = input_csv
        self.out_mensual_csv = out_mensual_csv
        self.out_ordenado_csv = out_ordenado_csv
        self.out_tendencias_csv = out_tendencias_csv
        self.pais_especifico = pais_especifico
        self.random_state = random_state

        self.df = None
        self.mensual = None

    # PASO 1
    def load_and_parse_dates(self):
        self.df = pd.read_csv(self.input_csv)
        self.df['Fecha'] = pd.to_datetime(self.df['Fecha'], errors='coerce')

        print("\n")
        print("PASO 1: Convertir la columna de fecha a datetime")
        print("\nFechas no parseadas (NaT):", self.df['Fecha'].isna().sum())
        print(self.df.dtypes)
        print(self.df.sort_values('Fecha').head())

    # PASO 2
    def build_monthly_aggregation(self):
        self.df['Mes'] = self.df['Fecha'].dt.to_period('M').dt.to_timestamp()
        self.mensual = (
            self.df.groupby(['Pais', 'Mes'], as_index=False)['Casos Diarios']
                .sum()
                .rename(columns={'Casos Diarios': 'Casos Mensuales'})
        )

        print("\n")
        print("PASO 2: Agregar columna de Mes (primer día del mes) y agrupar por País y Mes")
        print("\nResumen mensual por país (muestra):")
        print(self.mensual.head(12))
        # Guardar
        self._save_csv(self.mensual, self.out_mensual_csv)
        print(f"Archivo guardado: {self.out_mensual_csv}")

    # PASO 3
    def sort_and_export(self):
        print("\nPASO 3: Ordenar por País y Fecha y exportar CSV")
        ordenado = self.df.sort_values(["Pais", "Fecha"])  # 'Fecha' ya está en datetime
        print(ordenado.head(10))
        ordenado.to_csv(self.out_ordenado_csv, index=False)
        print(f"Archivo guardado: {self.out_ordenado_csv}")

    # PASO 4
    def select_countries_and_trends(self):
        print("\nPASO 4: Seleccionar 2 países aleatorios + 'El Salvador' y analizar tendencias mensuales")
        paises_unicos = sorted(self.df['Pais'].dropna().unique().tolist())
        candidatos = [p for p in paises_unicos if p != self.pais_especifico]

        n_rand = 2 if len(candidatos) >= 2 else len(candidatos)
        paises_random = pd.Series(candidatos).sample(n=n_rand, random_state=self.random_state).tolist() if n_rand > 0 else []
        seleccionados = [self.pais_especifico] + paises_random
        print("Países seleccionados:", seleccionados)

        tendencias = (
            self.mensual[self.mensual['Pais'].isin(seleccionados)]
                .sort_values(['Pais', 'Mes'])
                .reset_index(drop=True)
        )

        tendencias['MM3'] = (
            tendencias.groupby('Pais')['Casos Mensuales']
                      .transform(lambda s: s.rolling(window=3, min_periods=1).mean())
        )

        print("\nTendencias (muestra):")
        print(tendencias.head(18))

        # Guardar en subcarpeta
        os.makedirs(os.path.dirname(self.out_tendencias_csv), exist_ok=True)
        tendencias.to_csv(self.out_tendencias_csv, index=False)
        print(f"Archivo guardado: {self.out_tendencias_csv}")

    # PASO 5
    def aggregate_by_calendar_month(self):
        print("\nPASO 5: (Seleccionados) Agregar por país y número de mes (01-12) desde tendencias_paises.csv y exportar JSON")
        # Leer únicamente los países seleccionados desde el CSV de tendencias
        df_trend = pd.read_csv(self.out_tendencias_csv, parse_dates=['Mes'])
        tmp = df_trend[['Pais', 'Mes', 'Casos Mensuales']].copy()
        tmp['MesNum'] = tmp['Mes'].dt.month
        total_mes_pais = (
            tmp.groupby(['Pais', 'MesNum'], as_index=False)['Casos Mensuales']
               .sum()
               .rename(columns={'Casos Mensuales': 'Casos Totales'})
        )
        nombres = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
            7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }
        total_mes_pais['MesNombre'] = total_mes_pais['MesNum'].map(nombres)
        total_mes_pais = total_mes_pais.sort_values(['Pais', 'MesNum']).reset_index(drop=True)

        # Mostrar y exportar a JSON
        print("\nTotales por país y mes (muestra, solo países seleccionados):")
        print(total_mes_pais.head(24))
        os.makedirs('analisis', exist_ok=True)
        salida = 'analisis/total_mensual_por_pais.json'
        total_mes_pais.to_json(salida, orient='records', force_ascii=False)
        print(f"Archivo JSON guardado: {salida}")

    # PASO 6
    def plot_interactive_evolution(self, json_path: str = 'analisis/total_mensual_por_pais.json'):
        print("\nPASO 6: Crear gráfico de líneas interactivo (Plotly) desde JSON")
        import json
        if not os.path.exists(json_path):
            print(f"No se encontró {json_path}. Generando datos a partir del PASO 5...")
            self.aggregate_by_calendar_month()
        # Cargar JSON
        df_json = pd.read_json(json_path, orient='records')
        # Asegurar orden de meses
        nombres = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
            7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }
        if 'MesNombre' not in df_json.columns:
            df_json['MesNombre'] = df_json['MesNum'].map(nombres)
        # Ordenado
        df_json = df_json.sort_values(['Pais', 'MesNum'])
        # Gráfico
        fig = px.line(
            df_json,
            x='MesNombre', y='Casos Totales', color='Pais',
            markers=True,
            title='Evolución mensual de casos por país (agregado por mes calendario)'
        )
        # Formato de números más específico y hover mejorado
        fig.update_traces(mode='lines+markers', hovertemplate='<b>%{legendgroup}</b><br>Mes=%{x}<br>Casos Totales=%{y:,.2f}<extra></extra>')
        fig.update_layout(
            xaxis_title='Mes',
            yaxis_title='Casos Totales',
            legend_title='País',
            hovermode='x unified',
            yaxis_tickformat=',.2f'
        )
        # Guardar HTML
        os.makedirs('analisis', exist_ok=True)
        out_html = 'analisis/evolucion_mensual_por_pais.html'
        fig.write_html(out_html, include_plotlyjs='cdn')
        print(f"Gráfico interactivo guardado: {out_html}")

    # PASO 7
    def export_global_peak_country(self):
        print("\nPASO 7: País con el mayor pico de casos diarios y su mes")
        if self.df is None:
            raise ValueError("DataFrame no cargado. Ejecuta load_and_parse_dates() primero.")
        # índice del pico global
        idx = self.df['Casos Diarios'].idxmax()
        fila = self.df.loc[idx]
        fecha = pd.to_datetime(fila['Fecha'])
        nombres = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
            7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }
        out = pd.DataFrame([
            {
                'Pais': fila['Pais'],
                'FechaPico': fecha.date(),
                'MesNum': fecha.month,
                'MesNombre': nombres.get(fecha.month),
                'Casos Pico': float(fila['Casos Diarios'])
            }
        ])
        os.makedirs('analisis', exist_ok=True)
        ruta = 'analisis/pico_maximo_global.csv'
        out.to_csv(ruta, index=False)
        print(f"Archivo guardado: {ruta}")

    # PASO 8
    def export_el_salvador_2020(self):
        print("\nPASO 8: Comportamiento de casos en El Salvador durante 2020 (agregado mensual)")
        if self.df is None:
            raise ValueError("DataFrame no cargado. Ejecuta load_and_parse_dates() primero.")
        df_es = self.df[(self.df['Pais'] == self.pais_especifico) & (self.df['Fecha'].dt.year == 2020)].copy()
        if df_es.empty:
            print("No se encontraron datos para El Salvador en 2020.")
            return
        df_es['Mes'] = df_es['Fecha'].dt.to_period('M').dt.to_timestamp()
        mensual_es = (
            df_es.groupby('Mes', as_index=False)['Casos Diarios']
                 .sum()
                 .rename(columns={'Casos Diarios': 'Casos Mensuales'})
        )
        os.makedirs('analisis', exist_ok=True)
        ruta = 'analisis/el_salvador_2020.csv'
        mensual_es.to_csv(ruta, index=False)
        print(f"Archivo guardado: {ruta}")

    # PASO 9
    def export_comparison_el_salvador_vs(self, otros: list[str] | None = None):
        print("\nPASO 9: Comparación de El Salvador con otros 2 países (agregado mensual por año-mes)")
        if self.df is None:
            raise ValueError("DataFrame no cargado. Ejecuta load_and_parse_dates() primero.")
        # Elegir países por defecto
        if otros is None:
            candidatos = set(self.df['Pais'].dropna().unique().tolist())
            candidatos.discard(self.pais_especifico)
            preferidos = [p for p in ['Guatemala', 'Honduras'] if p in candidatos]
            restantes = list(candidatos.difference(set(preferidos)))
            faltan = 2 - len(preferidos)
            if faltan > 0 and restantes:
                preferidos += pd.Series(restantes).sample(n=min(faltan, len(restantes)), random_state=self.random_state).tolist()
            otros = preferidos[:2]
        seleccionados = [self.pais_especifico] + (otros or [])
        tmp = self.df[self.df['Pais'].isin(seleccionados)].copy()
        tmp['Anio'] = tmp['Fecha'].dt.year
        tmp['MesNum'] = tmp['Fecha'].dt.month
        tmp['Mes'] = tmp['Fecha'].dt.to_period('M').dt.to_timestamp()
        comp = (
            tmp.groupby(['Pais', 'Anio', 'MesNum', 'Mes'], as_index=False)['Casos Diarios']
               .sum()
               .rename(columns={'Casos Diarios': 'Casos Mensuales'})
               .sort_values(['Pais', 'Anio', 'MesNum'])
        )
        nombres = {1:'Enero',2:'Febrero',3:'Marzo',4:'Abril',5:'Mayo',6:'Junio',7:'Julio',8:'Agosto',9:'Septiembre',10:'Octubre',11:'Noviembre',12:'Diciembre'}
        comp['MesNombre'] = comp['MesNum'].map(nombres)
        os.makedirs('analisis', exist_ok=True)
        ruta = 'analisis/comparacion_el_salvador_vs.csv'
        comp.to_csv(ruta, index=False)
        print(f"Países comparados: {seleccionados}")
        print(f"Archivo guardado: {ruta}")

    def run_all(self):
        self.load_and_parse_dates()
        self.build_monthly_aggregation()
        self.sort_and_export()
        self.select_countries_and_trends()
        self.aggregate_by_calendar_month()
        self.plot_interactive_evolution()
        self.export_global_peak_country()
        self.export_el_salvador_2020()
        self.export_comparison_el_salvador_vs()

    @staticmethod
    def _save_csv(df: pd.DataFrame, path: str):
        df.to_csv(path, index=False)


if __name__ == "__main__":
    pipeline = CovidTemporalAnalysis(
        input_csv="dataset_covid.csv",
        out_mensual_csv="casos_mensuales_por_pais.csv",
        out_ordenado_csv="dataset_covid_ordenado.csv",
        out_tendencias_csv="analisis/tendencias_paises.csv",
        pais_especifico="El Salvador",
        random_state=42,
    )
    pipeline.run_all()
