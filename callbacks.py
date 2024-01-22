import pandas as pd
from dash import html, dcc, Output, Input

df = pd.read_parquet('datasets\jobs.parquet')
# Sélectionner les 10 premières lignes
df_subset = df.sample(n=100, random_state=42)

def update_tabs(tab_selected, job_selected, region_selected, competence_selected, logiciel_selected, contrat_selected):
    # Filtrer les données en fonction des sélections
    filtered_df = filter_data(df_subset, job_selected, region_selected, competence_selected, logiciel_selected, contrat_selected)

    if tab_selected == 'tab1':
        # Colonnes que l'on veut afficher 
        columns_to_display = ['date_publication', 'contrat', 'intitule', 'entreprise', 'ville']
        
        # Dictionnaire de correspondance entre les noms actuels et les noms souhaités
        column_name_mapping = {
            'date_publication': 'Date de publication',
            'contrat': 'Type de contrat',
            'intitule': 'Intitulé du poste',
            'entreprise': 'Nom de l\'entreprise',
            'ville': 'Ville'
        }
        
        # Contenu de div avec les données filtrées
        data_rows = [html.Div([html.Div(filtered_df.iloc[i][col], className='cell', style={'text-align': 'center'}) for col in columns_to_display], className='data-row') for i in range(len(filtered_df))]
        
        return html.Div([
            html.H3('Voici les résultats de votre recherche : '),
            # En-tête de div avec les noms modifiés
            html.Div([html.Div(column_name_mapping[col], className='header-cell', style={'text-align': 'center'}) for col in columns_to_display], className='header-row'),
            # Contenu de div avec les données filtrées
            *data_rows
        ], className='table-container')
        
    elif tab_selected == 'tab2':
        return html.Div([
            html.H3("Dashboard"),
            html.H1("Affichage des 10 premières lignes d'une DataFrame depuis un fichier Parquet"),
            # Utilisation du composant Table pour afficher la DataFrame
            html.Table(
                # En-tête de tableau
                [html.Tr([html.Th(col) for col in df_subset.columns])] +
                # Contenu de tableau
                [html.Tr([html.Td(df_subset.iloc[i][col]) for col in df_subset.columns]) for i in range(len(df_subset))]
            ),
            # Ajoutez d'autres composants pour l'onglet 2 si nécessaire
        ])

def filter_data(df, job_selected, region_selected, competence_selected, logiciel_selected, contrat_selected):
    # Appliquer les filtres en fonction des sélections
    filtered_df = df[df['intitule'].str.contains('|'.join(job_selected), case=False, na=False) & 
                     df['contrat'].str.contains('|'.join(contrat_selected), case=False, na=False) & 
                     df['ville'].str.contains('|'.join(region_selected), case=False, na=False)]
    # Vous pouvez ajouter d'autres conditions de filtrage ici
    return filtered_df

