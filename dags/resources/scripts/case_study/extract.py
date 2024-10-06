def extract_logic(item, **kwargs):
    import os
    import pandas as pd
    from datetime import datetime
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Menghubungkan ke PostgreSQL dan MySQL menggunakan SQLAlchemy engine
    postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
    mysql_hook = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()

    # Membuat query untuk mengekstrak data dari MySQL
    query = f"SELECT * FROM case_study.{item['table']}"
    condition = []

    # Memeriksa apakah tabel ada di PostgreSQL
    with postgres_hook.connect() as conn:
        table_exists = pd.read_sql(
            f"SELECT to_regclass('bronze.{item['table']}')", conn
        ).iloc[0, 0]
        ingest_type = (
            kwargs["var"]["json"]
            .get("case_study_ingest_type", {})
            .get(item["table"], "full")
        )

        if table_exists and ingest_type == "incremental":
            # Menambahkan kondisi untuk incremental load
            for column in item.get("incremental_based_on", []):
                condition.append(
                    f"{column} BETWEEN '{kwargs['data_interval_start']}' AND '{kwargs['data_interval_end']}'"
                )

    if condition:
        query += f" WHERE {' OR '.join(condition)}"

    print("Query:", query)

    # Mengekstrak data dari MySQL
    with mysql_hook.connect() as conn:
        df = pd.read_sql(query, conn)
        df["md_extracted_at"] = datetime.now()

        # Menyimpan data ke Parquet
        os.makedirs("data/case_study", exist_ok=True)
        df.to_parquet(f"data/case_study/{item['table']}.parquet", index=False)
