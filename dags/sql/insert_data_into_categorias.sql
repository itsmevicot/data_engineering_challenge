CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.TB_CATEGORIA (
    id serial PRIMARY KEY,
    nome_categoria VARCHAR NOT NULL CHECK (nome_categoria <> '')
);

{% set rows = ti.xcom_pull(task_ids='task_extract_parquet_file_from_gcs') %}

{% for row in rows.iterrows() %}
    INSERT INTO public.TB_CATEGORIA (id, nome_categoria)
    VALUES ({{ row[1]['id'] }}, '{{ row[1]['nome_categoria'] }}')
    ON CONFLICT (id) DO UPDATE
    SET nome_categoria = EXCLUDED.nome_categoria
    WHERE TB_CATEGORIA.nome_categoria <> EXCLUDED.nome_categoria;
{% endfor %}
