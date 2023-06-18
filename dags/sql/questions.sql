-- Media do valor de venda

SELECT AVG(venda) AS valor_medio_venda
FROM public.tb_vendas;

-- Funcionários por ordem de maior número de vendas

SELECT f.nome AS funcionario, COUNT(*) AS total_vendas
FROM public.tb_vendas v
JOIN public.tb_funcionario f ON v.id_funcionario = f.id
GROUP BY f.nome
ORDER BY total_vendas DESC;

-- Funcionário que teve a maior venda

SELECT f.nome AS funcionario, v.venda AS maior_venda
FROM public.tb_funcionario f
JOIN public.tb_vendas v ON f.id = v.id_funcionario
WHERE v.venda = (
    SELECT MAX(venda)
    FROM public.tb_vendas
);

-- Categorias mais vendidas (em quantidade)

SELECT c.nome_categoria AS category_name, COUNT(*) AS total_sales
FROM public.tb_vendas v
JOIN public.tb_categoria c ON v.id_categoria = c.id
GROUP BY c.nome_categoria
ORDER BY total_sales DESC;

