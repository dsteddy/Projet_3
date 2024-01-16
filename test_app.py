import dash
from dash import html, dcc, Input, Output, callback
from dash import dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

links = [
        "datasets/pole_emploi_data analyst.parquet",
        "datasets/pole_emploi_data engineer.parquet",
        "datasets/pole_emploi_data scientist.parquet",
        "datasets/WTTJ_data analyst.parquet",
        "datasets/WTTJ_data engineer.parquet",
        "datasets/WTTJ_data scientist.parquet",
]

pe_analyst = pd.read_parquet(links[0])
pe_engineer = pd.read_parquet(links[1])
pe_scientist = pd.read_parquet(links[2])
wttj_analyst = pd.read_parquet(links[3])
wttj_engineer = pd.read_parquet(links[4])
wttj_scientist = pd.read_parquet(links[5])

app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

type_contrat = dcc.Dropdown(
        id = "type_contrat",
        options = [
                {"label" : "CDI", "value" : "CDI"},
                {"label" : "CDD", "value" : "CDD"},
                {"label" : "Stage", "value" : "Stage"},
                {"label" : "Alternance", "value" : "Alternance"},
        ],
        multi = True,
)

job_title = dcc.Dropdown(
        id = "job_title",
        options = [
                {"label" : "Data Analyst", "value" : "data analyst"},
                {"label" : "Data Scientist", "value" : "data scientist"},
                {"label" : "Data Engineer", "value" : "data engineer"},
        ],
        multi = True,
)

tableau = dash_table.DataTable(
    id='type_contrat_output',
    columns=[{'name': col, 'id': col} for col in wttj_analyst.columns],
    data=wttj_analyst.to_dict('records'),
)

city = dcc.Dropdown(
        id = "ville",
        options = []
)

app.layout = dbc.Container(
        [
                # Titre
                dbc.Row(
                        [
                                dbc.Col(html.H1("JOB'ARTS"), width={"size":6,"offset":3})
                        ]
                ),
                # Espace
                dbc.Row(
                        [
                                dbc.Col(html.Div(style={"height":"150px"})),
                        ]
                ),
                # Sélecteur Job Title
                dbc.Row(
                        [
                                dbc.Col(html.H4("Job :"), width = {"size":2}),
                                dbc.Col(job_title, width = {"size":2}),
                    ]
                ),
                # Espace
                dbc.Row(
                        [
                                dbc.Col(html.Div(style={"height":"50px"})),
                        ]
                ),
                # Sélecteur Type Contrat
                dbc.Row(
                        [
                                dbc.Col(html.H4("Type de contrat :"), width = {"size":2}),
                                dbc.Col(type_contrat, width = {"size":2}),
                                # dbc.Col(html.Div(id='type_contrat_output'))
                        ],
                ),
                # Espace
                dbc.Row(
                        [
                                dbc.Col(html.Div(style={"height":"50px"})),
                        ]
                ),
                # Tableau
                dbc.Row(
                        [
                                dbc.Col(tableau)
                        ]
                ),
    ],
    fluid = True,
)

@app.callback(
    Output('type_contrat_output', 'data'),
    Input('type_contrat', 'value')
)

def select_type_contrat(value):
    if not value:
        return wttj_analyst.to_dict('records')

    filtered_df = wttj_analyst[wttj_analyst["contrat"].isin(value)]
    return filtered_df.to_dict('records')

if __name__ == '__main__':
            app.run(debug=True)