import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import MultiLabelBinarizer


movies_filt = pd.read_parquet('datasets/movie_modelo.parquet')

#PREPROCESAMIENTO
# Convertir columna 'genres' de una cadena de texto a una lista
movies_filt['genres'] = movies_filt['genres'].apply(lambda x: x.split(','))  # Asegúrate de que los géneros estén separados por comas


# Se codifica los géneros a binario
mlb = MultiLabelBinarizer()
genres_encoded = mlb.fit_transform(movies_filt['genres'])
genres_df = pd.DataFrame(genres_encoded, columns=mlb.classes_)


# Seleccionar los features numericos
features_num = movies_filt[['vote_average', 'popularity']]

# Se normaliza escalando
scaler = StandardScaler()
features_num_escaldas = scaler.fit_transform(features_num)
numeric_df = pd.DataFrame(features_num_escaldas, columns=features_num.columns)

# Se concatena los features normalizados y escalados
features_df = pd.concat([genres_df.reset_index(drop=True), numeric_df.reset_index(drop=True)], axis=1)

# calculo de la matriz
cosine_simi = cosine_similarity(features_df, features_df)

# FUNCION DE RECOMENDACION

def recomendacion(titulo, df):

    # Normalizar el título para la búsqueda
    titulo = titulo.title()

    titulo_row = df[df["title"] == titulo ]
    
    # Se verifica si la película existe en la base de datos
    if titulo_row.empty:
        return 0

    # Se obtiene el indice dado el titulo
    idx = titulo_row.index[0]

    # Obtengo los puntajes de similitud 
    sim_scores = list(enumerate(cosine_simi[idx]))

    # Se ordena según la similitud de puntajes en orden descendente
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    # Indices de las películas similares
    sim_indices = [i[0] for i in sim_scores if i[0] != idx]

    # Se verifica si peliculas similares
    if len(sim_indices) == 0:
        return f"No se encontraron películas similares a '{titulo}'."

    # Obtengo los titulos de las 5 películas más similares
    top_movies = df['title'].iloc[sim_indices[:5]].values.tolist()

    return top_movies

titulo_pelicula = 'Toy Story'
recomendaciones = recomendacion(titulo_pelicula, movies_filt)






