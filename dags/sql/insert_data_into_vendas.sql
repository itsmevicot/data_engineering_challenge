CREATE SCHEMA IF NOT EXISTS public;


CREATE TABLE IF NOT EXISTS public.TB_VENDAS (
    id_venda serial PRIMARY KEY,
    id_funcionario INTEGER NOT NULL REFERENCES public.TB_FUNCIONARIO (id),
    id_categoria INTEGER NOT NULL REFERENCES public.TB_CATEGORIA (id),
    data_venda DATE NOT NULL,
    venda INTEGER NOT NULL CHECK (venda <> 0)
);

{% set rows = ti.xcom_pull(task_ids='task_transform_data_from_postgresql') %}

{% for row in rows.iterrows() %}
    INSERT INTO public.TB_VENDAS (id_venda, id_funcionario, id_categoria, data_venda, venda)
    VALUES ({{ row[1]['id_venda'] }}, {{ row[1]['id_funcionario'] }}, {{ row[1]['id_categoria'] }},
            '{{ row[1]['data_venda'] }}', {{ row[1]['venda'] }})
    ON CONFLICT (id_venda) DO UPDATE
    SET id_funcionario = EXCLUDED.id_funcionario,
        id_categoria = EXCLUDED.id_categoria,
        data_venda = EXCLUDED.data_venda,
        venda = EXCLUDED.venda;
{% endfor %}
