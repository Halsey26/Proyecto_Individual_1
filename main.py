from fastapi import FastAPI
import pandas as pd

app = FastAPI()

#lectura del archivo movie
movie_api = pd.read_parquet('../datasets/movie_dataset_final.parquet', engine= 'pyarrow')

    # X cantidad de películas fueron estrenadas en el mes de 
def f_filmaciones_mes(df, mes, column):
    mes= mes.capitalize()
    pelis_mes= df[df[column]==mes ]   

    len_films = len(pelis_mes)
    return f"{len_films} cantidad de películas fueron estrenadas en el mes de {mes}"


@app.get("/inicio")
async def ruta_prueba():
    return "Hola"


@app.get("/Cantidad_Filmacione_Mes/{mes}")
def cantidad_filmaciones_mes(mes:str):
    return {"message":f_filmaciones_mes(movie_api,mes,'release_meses')}