def load_logic(item, **kwargs):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Menghubungkan ke PostgreSQL menggunakan SQLAlchemy engine
    postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

    # Membaca data dari Parquet
    df = pd.read_parquet(f"data/case_study/{item['table']}.parquet")

    # Menghubungkan ke PostgreSQL dan memeriksa apakah tabel ada
    with postgres_hook.connect() as conn:
        table_exists = pd.read_sql(
            f"SELECT to_regclass('bronze.{item['table']}')", conn
        ).iloc[0, 0]
        ingest_type = (
            kwargs["var"]["json"]
            .get("case_study_ingest_type", {})
            .get(item["table"], "full")
        )

        if not table_exists or ingest_type == "full":
            # Jika tabel tidak ada, buat tabel baru di schema bronze
            df.to_sql(
                name=item["table"],
                con=conn,
                schema="bronze",
                if_exists="replace",
                index=False,
            )
            print("Load data ke bronze table berhasil")
        else:
            # Jika tabel ada, load data ke schema temp dan lakukan merge
            conn.execute(f"DROP TABLE IF EXISTS temp.{item['table']}")
            conn.execute(
                f"CREATE TABLE temp.{item['table']} (LIKE bronze.{item['table']})"
            )

            df.to_sql(
                name=item["table"],
                con=conn,
                schema="temp",
                if_exists="append",
                index=False,
            )
            print("Load data ke temp table berhasil")

            # Query untuk melakukan merge dari temp ke bronze
            query_merge = """
                MERGE INTO bronze.{main_table} M
                USING temp.{temp_table} T
                ON {merge_on}
                WHEN MATCHED THEN
                    UPDATE SET
                    {update_set}
                WHEN NOT MATCHED THEN
                    INSERT VALUES ({insert_val})
            """.format(
                main_table=item["table"],
                temp_table=item["table"],
                merge_on=" AND ".join(
                    f'M."{col}" = T."{col}"' for col in item["merge_on"]
                ),
                update_set=", ".join(f'"{col}" = T."{col}"' for col in df.columns),
                insert_val=", ".join(f'"{col}"' for col in df.columns),
            )
            print("Query merge:", query_merge)

            conn.execute(query_merge)
            print("Merge data berhasil")
