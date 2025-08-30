import pandas as pd


class DataTransformer:
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        df["Hired"] = df.apply(lambda x: 1 if x["Code Challenge Score"]
                               >= 7 and x["Technical Interview Score"] >= 7 else 0, axis=1)
        return df

    @staticmethod
    def dim_date(df: pd.DataFrame) -> pd.DataFrame:
        """Crea la dimensión de fechas a partir del DataFrame original."""
        df["Application Date"] = pd.to_datetime(df["Application Date"])

        dim_date = df[["Application Date"]
                      ].drop_duplicates().reset_index(drop=True)
        dim_date["year"] = dim_date["Application Date"].dt.year
        dim_date["month"] = dim_date["Application Date"].dt.month
        dim_date["day"] = dim_date["Application Date"].dt.day

        return dim_date

    @staticmethod
    def dim_country(df: pd.DataFrame) -> pd.DataFrame:
        """Crea la dimensión de países a partir del DataFrame original."""
        dim_country = df[["Country"]].drop_duplicates().reset_index(drop=True)

        return dim_country

    @staticmethod
    def dim_candidate(df: pd.DataFrame) -> pd.DataFrame:
        """Crea la dimensión de candidatos a partir del DataFrame original."""
        dim_candidate = df[["First Name", "Last Name", "Email",
                            "YOE"]].drop_duplicates().reset_index(drop=True)

        return dim_candidate

    @staticmethod
    def dim_seniority(df: pd.DataFrame) -> pd.DataFrame:
        """Crea la dimensión de seniority a partir del DataFrame original."""
        dim_seniority = df[["Seniority"]
                           ].drop_duplicates().reset_index(drop=True)
        dim_seniority["level"] = dim_seniority["Seniority"].map(
            {'Intern': 1, 'Trainee': 2, 'Junior': 3, 'Mid-Level': 4, 'Senior': 5, 'Lead': 6, 'Architect': 7})

        return dim_seniority

    @staticmethod
    def dim_technology(df: pd.DataFrame) -> pd.DataFrame:
        """Crea la dimensión de seniority a partir del DataFrame original."""
        dim_technology = df[["Technology"]
                            ].drop_duplicates().reset_index(drop=True)

        return dim_technology
