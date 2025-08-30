import pandas as pd
from datawarehouse import DataWarehouse
from transform import DataTransformer


class DataLoader:
    def __init__(self, dw: DataWarehouse):
        self.dw = dw

    def load_dim_date(self, df: pd.DataFrame):
        dim_date = DataTransformer.dim_date(df)
        for _, row in dim_date.iterrows():
            self.dw.insert_data("dim_date", {
                "application_date": row["Application Date"],
                "year": int(row["year"]),
                "month": int(row["month"]),
                "day": int(row["day"])
            })

    def load_dim_country(self, df: pd.DataFrame):
        dim_country = DataTransformer.dim_country(df)
        for _, row in dim_country.iterrows():
            self.dw.insert_data("dim_country", {
                "country_name": row["Country"]
            })

    def load_dim_candidate(self, df: pd.DataFrame):
        dim_candidate = DataTransformer.dim_candidate(df)
        for _, row in dim_candidate.iterrows():
            self.dw.insert_data("dim_candidate", {
                "first_name": row["First Name"],
                "last_name": row["Last Name"],
                "email": row["Email"],
                "years_of_experience": int(row["YOE"])
            })

    def load_dim_seniority(self, df: pd.DataFrame):
        dim_seniority = DataTransformer.dim_seniority(df)
        for _, row in dim_seniority.iterrows():
            self.dw.insert_data("dim_seniority", {
                "senority_name": row["Seniority"],
                "senority_level": int(row["level"])
            })

    def load_dim_technology(self, df: pd.DataFrame):
        dim_technology = DataTransformer.dim_technology(df)
        for _, row in dim_technology.iterrows():
            self.dw.insert_data("dim_technology", {
                "technology_name": row["Technology"]
            })

    def load_fact_hires(self, df: pd.DataFrame):
        """Carga la tabla de hechos fact_hires resolviendo las FK con lookups"""
        for _, row in df.iterrows():

            # Buscar IDs en cada dimensión
            date_id = self._get_id(
                "dim_date", "application_date", row["Application Date"], "date_id")
            country_id = self._get_id(
                "dim_country", "country_name", row["Country"], "country_id")
            candidate_id = self._get_id(
                "dim_candidate", "email", row["Email"], "candidate_id")
            technology_id = self._get_id(
                "dim_technology", "technology_name", row["Technology"], "technology_id")
            seniority_id = self._get_id(
                "dim_seniority", "senority_name", row["Seniority"], "senority_id")

            # Insertar en la tabla de hechos
            self.dw.insert_data("fact_hires", {
                "candidate_id": candidate_id,
                "date_id": date_id,
                "country_id": country_id,
                "technology_id": technology_id,
                "senority_id": seniority_id,
                "hired": row["Hired"],
                "code_challenge_score": row["Code Challenge Score"],
                "technical_interview_score": row["Technical Interview Score"]
            })

    def _get_id(self, table, column, value, id_column):
        """Obtiene el ID correspondiente de una dimensión dado un valor"""
        query = f"SELECT {id_column} FROM {table} WHERE {column} = %s"
        cursor = self.dw.conn.cursor(buffered=True)
        cursor.execute(query, (value,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[0]
        else:
            raise ValueError(f"No se encontró {value} en {table}.{column}")
