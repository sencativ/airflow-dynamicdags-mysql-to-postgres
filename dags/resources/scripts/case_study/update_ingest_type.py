def update_ingest_type_logic(item, **kwargs):
    from airflow.models.variable import Variable

    # Update variable dengan data yang baru
    list_ingest_type = kwargs["var"]["json"].get("case_study_ingest_type", {})
    list_ingest_type[item["table"]] = "incremental"

    Variable.set(
        key="case_study_ingest_type",
        value=list_ingest_type,
        serialize_json=True,
    )
