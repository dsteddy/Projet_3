import pandas as pd
import nltk
from nltk import FreqDist
import matplotlib.pyplot as plt
from wordcloud import WordCloud

nltk.download('popular')

description = """Les missions du poste
Dans le cadre d'une forte croissance, notre client qui évolue dans la pose de panneaux photovoltaïque, recherche à créer un pôle data. En conséquence, ce dernier souhaite recruter un data analyst supplémentaire ainsi qu'un data scientist. Les missions proposées sont les suivantes :
- Analyse de données
- Mise en place de tableau de bord
- Mette en place des modèles sur de la maintenance prédictive
- Projection de sur la maintenance, évolution du carnet de commander et consommation énergétique

Le profil recherché
- Vous êtes autonome
- Vous justifiez d'une précédente expérience sur un poste similaire d'au moins 3 ans
- Techniquement, vous êtes à l'aise
- Cela est un vrai plus si vous avez déjà fait du développement applicatif
- Maîtrise de Odata, powerbi

Bienvenue chez Peaks
QUI SOMMES NOUS ?

Peaks est une société de conseil, management et création de solutions numériques innovantes. Présente à Lyon, Paris, Reims et Aix-en-Provence, nous animons une communauté de talents passionnés par les technologies et les nouvelles pratiques dans le monde du numérique.

NOS ENGAGEMENTS

Accompagnement : une équipe dédiée spécifiquement pour vous guider et vous faire progresser tout au long de votre parcours chez Peaks : mise en place d'un suivi personnalisé, un centre de formation certifié Qualiopi, du mentoring, des participations à des conférences (MixIT, SF Live, WeLoveSpeed, Agile Lyon...) et des meetups, des communautés techniques pour échanger.

Environnement de travail : culture du respect et de l'échange, beaucoup d'événements en équipe (participation à des courses solidaires, rencontres sportives, apéros mensuels), bonne ambiance.

Équilibre vie pro / vie perso : projets près de chez vous et bureaux en plein centre, adaptabilité, possibilité d'horaires flexibles, home office et missions en remote.

Projets d'entreprise : projets internes pour mettre nos compétences numériques au service de causes qui ont du sens via notre cellule Recherche et Innovation

Inclusion et Diversité : un environnement accueillant pour tous les salariés et une équipe véritablement diverse qui nous apporte richesse et performance

Autres Avantages au delà du plaisir de nous rejoindre (bien évidemment ;-)) : prime de vacances, tickets restaurants, RTT, chèques cadeaux, mutuelle..."""

def nuage_de_mots (texte):
    stop_words = nltk.corpus.stopwords.words("french")
    texte_words = nltk.word_tokenize(texte)
    stop_ponc = ['.',',',':','"',"'",'(',')','’','»','«','“']
    texte_clean = []
    
    for word in texte_words:
        if word.lower() not in stop_words and word not in stop_ponc:
            texte_clean.append(word)
   
    texte_freqdist = FreqDist(texte_clean)
    wordcloud = WordCloud(width=480, height=480, max_font_size=200, min_font_size=10).generate_from_frequencies(texte_freqdist)
    return wordcloud
    
plt.figure()
plt.imshow(nuage_de_mots(description), interpolation="bilinear")
plt.axis("off")
plt.margins(x=0, y=0)
plt.show()

