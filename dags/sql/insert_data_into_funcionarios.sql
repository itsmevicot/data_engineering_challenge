CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.TB_FUNCIONARIO (
    id serial PRIMARY KEY,
    nome VARCHAR NOT NULL CHECK (nome <> '')
);

{% set rows = ti.xcom_pull(task_ids='task_transform_data_from_api') %}

{% for row in rows.iterrows() %}
    INSERT INTO public.TB_FUNCIONARIO (id, nome)
    VALUES ({{ row[1]['id'] }}, '{{ row[1]['nome_funcionario'] }}')
    ON CONFLICT (id) DO UPDATE
    SET nome = EXCLUDED.nome
    WHERE TB_FUNCIONARIO.nome <> EXCLUDED.nome;
{% endfor %}
