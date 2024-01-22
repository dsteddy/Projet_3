import pandas as pd
from dash import html, dcc, Output, Input

df = pd.read_parquet('assets\offre.parquet')
# Sélectionner les 10 premières lignes
df_subset = df.head(10)

def update_tabs(tab_selected, job_selected, region_selected, competence_selected, logiciel_selected, contrat_selected):
    if tab_selected == 'tab1':
        return html.Div([
            html.H3(f'Résultat pour le métier : {job_selected}'),
            html.P(f'Dans la région : {region_selected}'),
            html.P(f'Compétences requises : {", ".join(competence_selected)}' if competence_selected else 'Aucune compétence sélectionnée'),
            html.P(f'Logiciels maîtrisés : {", ".join(logiciel_selected)}' if logiciel_selected else 'Aucun logiciel sélectionné'),
            html.P(f'Types de contrat : {", ".join(contrat_selected)}' if contrat_selected else 'Aucun type de contrat sélectionné'),
            # Ajoutez d'autres composants pour l'onglet 1 si nécessaire
        ])
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
