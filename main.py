import pandas as pd
from datawarehouse import DataWarehouse
from load import DataLoader
from transform import DataTransformer


def main():
    # Cargar el dataset original
    df = pd.read_csv("data/candidates.csv", sep=";")
    df = DataTransformer.clean_data(df)

    # Conectar al Data Warehouse
    dw = DataWarehouse(
        host="localhost",
        user="root",
        password="root",
        database="hires_model"
    )
    dw.connect()

    # Crear el loader
    loader = DataLoader(dw)

    # Cargar dimensiones
    loader.load_dim_date(df)
    loader.load_dim_country(df)
    loader.load_dim_candidate(df)
    loader.load_dim_seniority(df)
    loader.load_dim_technology(df)

    # Cargar la tabla de hechos
    loader.load_fact_hires(df)

    # Cerrar conexión
    dw.close()


if __name__ == "__main__":
    main()
