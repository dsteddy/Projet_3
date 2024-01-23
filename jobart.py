import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from callbacks import update_tabs # fichier avec mes fonctions
from tools import soft_skills_list, tech_skills_list # fichier avec les fonctions globales

app = dash.Dash(__name__)

df = pd.read_parquet('datasets\jobs.parquet')
# Sélectionner les 10 premières lignes
df_subset = df.sample(n=100, random_state=42)

# Options de la liste déroulante pour le métier
job_options = ['Data analyst', 'data engineer', 'Data Scientist', 'Consultant Data']

# Options de la liste déroulante pour les régions ou ville 
region_options = df['ville'].unique()

# region_options = [
#     'Auvergne-Rhône-Alpes', 'Bourgogne-Franche-Comté', 'Bretagne', 'Centre-Val de Loire', 'Corse',
#     'Grand Est', 'Hauts-de-France', 'Île-de-France', 'Normandie', 'Nouvelle-Aquitaine', 'Occitanie',
#     'Pays de la Loire', 'Provence-Alpes-Côte d\'Azur'
# ]

# Options de la liste à choix multiple pour les compétences
competence_options = soft_skills_list()

# Options de la liste à choix multiple pour les logiciels
logiciel_options = tech_skills_list()

# Options de la liste à choix multiple pour le type de contrat
contrat_options = ['CDI', 'CDD', 'alternance', 'stage']

app.layout = html.Div(
    children=[
        # Ajoutez un logo à gauche du titre
        html.Div([
            html.Img(src='assets/jobarts.png', style={'width': '100px', 'height': '100px', 'borderRadius': '50%'}),
            html.H1("Jobart's", style={'textAlign': 'center', 'fontWeight': 'bold', 'marginLeft': '20px'}),
        ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center', 'marginBottom': '20px'}),

        # Espace au début de l'application
        html.Div(style={'height': '20px'}),

        # Conteneur principal
        html.Div(
            children=[
                # Première ligne avec trois listes déroulantes
                html.Div([
                    # Titre et liste déroulante pour le métier sur la même ligne
                    html.Div([
                        html.H4("Métier Recherche"),
                        dcc.Dropdown(
                            id='job-dropdown',
                            options=[{'label': job, 'value': job} for job in job_options],
                            value=[],  # Aucune sélection par défaut (liste à choix multiple)
                            multi=True,  # Liste à choix multiple
                            style={'width': '30%', 'minWidth': '300px'},  # Largeur de 30% avec une largeur minimale
                            placeholder='Sélectionnez un ou plusieurs métiers'
                        ),
                    ], style={'display': 'inline-block', 'width': '30%', 'marginRight': '50px'}),

                    # Titre et liste déroulante pour les régions sur la même ligne
                    html.Div([
                        html.H4("Région"),
                        dcc.Dropdown(
                            id='region-dropdown',
                            options=[{'label': region, 'value': region} for region in region_options],
                            value=[],  # Aucune sélection par défaut (liste à choix multiple)
                            multi=True,  # Liste à choix multiple
                            style={'width': '30%', 'minWidth': '300px', 'marginRight': '50px'},  # Largeur de 30% avec une largeur minimale
                            placeholder='Sélectionnez une ou plusieurs régions'
                        ),
                    ], style={'display': 'inline-block', 'width': '30%'}),

                    # Titre et liste à choix multiple pour les compétences sur la même ligne
                    html.Div([
                        html.H4("Compétence"),
                        dcc.Dropdown(
                            id='competence-dropdown',
                            options=[{'label': competence, 'value': competence} for competence in competence_options],
                            value=[],  # Aucune sélection par défaut (liste à choix multiple)
                            multi=True,  # Liste à choix multiple
                            style={'width': '30%', 'minWidth': '300px'},  # Largeur de 30% avec une largeur minimale
                            placeholder='Sélectionnez une ou plusieurs compétences'
                        ),
                    ], style={'display': 'inline-block', 'width': '30%', 'marginTop': '10px', 'marginRight': '20px'}),
                ]),

                # Deuxième ligne avec deux listes déroulantes
                html.Div([
                    # Titre et liste à choix multiple pour les logiciels sur la même ligne
                    html.Div([
                        html.H4("Logiciel"),
                        dcc.Dropdown(
                            id='logiciel-dropdown',
                            options=[{'label': logiciel, 'value': logiciel} for logiciel in logiciel_options],
                            value=[],  # Aucune sélection par défaut (liste à choix multiple)
                            multi=True,  # Liste à choix multiple
                            style={'width': '30%', 'minWidth': '300px', 'marginRight': '300px'},  # Largeur de 30% avec une largeur minimale
                            placeholder='Sélectionnez un ou plusieurs logiciels'
                        ),
                    ], style={'display': 'inline-block', 'width': '30%', 'marginTop': '10px'}),

                    # Titre et liste à choix multiple pour le type de contrat sur la même ligne
                    html.Div([
                        html.H4("Type de Contrat"),
                        dcc.Dropdown(
                            id='contrat-dropdown',
                            options=[{'label': contrat, 'value': contrat} for contrat in contrat_options],
                            value=[],  # Aucune sélection par défaut (liste à choix multiple)
                            multi=True,  # Liste à choix multiple
                            style={'width': '30%', 'minWidth': '300px'},  # Largeur de 30% avec une largeur minimale
                            placeholder='Sélectionnez un ou plusieurs type de contrat'
                        ),
                    ], style={'display': 'inline-block', 'width': '30%', 'marginTop': '10px'}),
                    
                  # Sélecteur de temps (date)
                    html.Div([
                        html.H4("Date de Publication"),
                        dcc.DatePickerRange(
                            id='date-picker-range',
                            start_date=df_subset['date_publication'].min(),  # Date de début basée sur la valeur minimale dans le DataFrame
                            end_date=df_subset['date_publication'].max(),  # Date de fin basée sur la valeur maximale dans le DataFrame
                            display_format='DD/MM/YYYY',  # Format d'affichage de la date
                            style={'fontSize': 14, 'display': 'inline-block', 'width': '70%'},  # Ajustements de style pour la taille du texte
                        ),
                    ], style={'display': 'inline-block', 'width': '30%', 'marginTop': '10px'}),
                ]),
                
                # Espace avant les onglets
                html.Div(style={'height': '40px'}),

                # Onglets en dessous des listes déroulantes sur toute la largeur
                dcc.Tabs(id='tabs', value='tab1', children=[
                    dcc.Tab(label='Résultat', value='tab1'),
                    dcc.Tab(label='Dashboard', value='tab2'),
                ],
                    style={'width': '100%', 'marginTop': '10px'}  # Les onglets occupent toute la largeur avec un espace en haut
                ),
            ],
            style={'display': 'flex', 'flexWrap': 'wrap', 'flexDirection': 'column', 'alignItems': 'flex-start'}  # Alignement vertical en haut et flex-wrap pour le retour à la ligne
        ),

        # Contenu des onglets
        html.Div(id='tabs-content')
    ]
)

# Callback pour mettre à jour le contenu des onglets
@app.callback(
    Output('tabs-content', 'children'),
    [Input('tabs', 'value'),
     Input('job-dropdown', 'value'),
     Input('region-dropdown', 'value'),
     Input('competence-dropdown', 'value'),
     Input('logiciel-dropdown', 'value'),
     Input('contrat-dropdown', 'value')]
)

def update_tabs_callback(tab_selected, job_selected, region_selected, competence_selected, logiciel_selected, contrat_selected):
    return update_tabs(tab_selected, job_selected, region_selected, competence_selected, logiciel_selected, contrat_selected)

if __name__ == '__main__':
    app.run_server(debug=True)
