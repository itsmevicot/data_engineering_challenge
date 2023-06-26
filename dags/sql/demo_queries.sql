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

SELECT c.nome_categoria AS nome_categoria, COUNT(*) AS total_vendas
FROM public.tb_vendas v
JOIN public.tb_categoria c ON v.id_categoria = c.id
GROUP BY c.nome_categoria
ORDER BY total_vendas DESC;

-- Funcionários ordenados por valor total de vendas

SELECT f.nome AS funcionario, SUM(v.venda) AS valor_total_vendas
FROM public.tb_vendas v
JOIN public.tb_funcionario f ON v.id_funcionario = f.id
GROUP BY f.nome
ORDER BY valor_total_vendas DESC


-- Meses ordenados por valor total de vendas

SELECT EXTRACT(MONTH FROM data_venda) AS month, SUM(venda) AS total_vendas
FROM public.tb_vendas
WHERE EXTRACT(YEAR FROM data_venda) = 2017 -- Existem vendas entre 2017 e 2020
GROUP BY EXTRACT(MONTH FROM data_venda)
ORDER BY total_vendas DESC;


-- Categoria ordenadas por mais lucrativas

SELECT c.nome_categoria AS nome_categoria, SUM(v.venda) AS valor_total_vendas
FROM public.tb_vendas v
JOIN public.tb_categoria c ON v.id_categoria = c.id
GROUP BY nome_categoria
ORDER BY valor_total_vendas DESC

