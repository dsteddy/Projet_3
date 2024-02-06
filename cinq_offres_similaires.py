import pandas as pd
import sklearn as sklearn
import nltk
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# import du dataframe all_jobs
df = pd.read_parquet("datasets/all_jobs.parquet")

# rajout d'une colonne cl_descr = description nettoyee
df["cl_descr"] = np.nan
for i in range(df.index.stop) :
    texte = df["description"][i]
    mots = nltk.word_tokenize(texte)

    stop_words = nltk.corpus.stopwords.words("french")
    stop_ponc = [ ',' ,'!', '?', '(', ')', '[', ']', '-', '.', ':',';', '/', """'"""]

    cl_mots = []
    for word in mots:
        if word.lower() not in stop_words and word.lower() not in stop_ponc:
            cl_mots.append(word)
    print(cl_mots)
    #print(c_texte)
    df["cl_descr"][i] = cl_mots


#df_ml = df intermediaire avec selection des colonnes pour le ml
features = ['contrat', 'intitule', 'secteur_activite', 'experience', 'ville', 'tech_skills', 'soft_skills', 'cl_descr']
df_ml = df[features]

for feature in features:
  if df_ml[feature].isnull().any() or df_ml[feature].empty:
    df_ml[feature] = df[feature].fillna('', inplace=True)

df_ml["text_data"] = df_ml.fillna('').astype(str).apply(lambda x: ' '.join(x), axis=1)

df["text_ml"] = np.nan
for i in range(df.index.stop) :
    texte = df_ml["text_data"][i]
    mots = nltk.word_tokenize(texte)

    stop_words = nltk.corpus.stopwords.words("french")
    stop_ponc = [ ',' ,'!', '?', '(', ')', '[', ']', '-', '.', ':',';', '/','_',"'"]

    cl_mots = []
    for word in mots:
        if word.lower() not in stop_words and word.lower() not in stop_ponc:
            cl_mots.append(word)
    print(cl_mots)
    #print(c_texte)
    df["text_ml"][i] = cl_mots


df["text_ml"].apply(lambda x: ' '.join(x))

documents = df["text_ml"].astype(str).tolist()

tfidf = TfidfVectorizer()
count_matrix = tfidf.fit_transform(documents)

cosine_sim = cosine_similarity(count_matrix)

# index de l'offre que l'utilisateur aime :
user_likes_index = 24
# Get the cosine similarity scores pour l'offre que l'utilisateur aime
similarities = cosine_sim[user_likes_index]

# Get indices of offers sorted by similarity
similar_offers_indices = similarities.argsort()[::-1][1:6]

# Get the actual DataFrame rows for similar offers
similar_offers = df.loc[similar_offers_indices]

# Print
print(similar_offers_indices)
print(similar_offers)
