CREATE   PROCEDURE gold.sp_load_gold
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;   -- tudo ou nada: se algo falhar, nada fica pela metade

        -- 1) Esvazia: fatos primeiro, dimensões depois
        DELETE FROM gold.fact_order_items;
        DELETE FROM gold.fact_orders;
        DELETE FROM gold.fact_targets_monthly;
        DELETE FROM gold.dim_customer;
        DELETE FROM gold.dim_product;
        DELETE FROM gold.dim_date;

        -- 2) Dimensões
        INSERT INTO gold.dim_date
            (date_key, [date], [year], month_num, month_name, month_start, day_of_week, weekday_num, is_partial_month)
        SELECT date_key, [date], [year], month_num, month_name, month_start, day_of_week, weekday_num, is_partial_month
        FROM lh_aurum_silver.dbo.silver_calendar;

        INSERT INTO gold.dim_customer (user_id, segment, city, [state], customer_label)
        SELECT user_id, segment, city, [state],
               -- "restaurant" + 37404863 -> "Restaurant 4863"
               CONCAT(UPPER(LEFT(segment, 1)), SUBSTRING(segment, 2, 50), ' ',
                      RIGHT(CAST(user_id AS VARCHAR(20)), 4))
        FROM lh_aurum_silver.dbo.silver_customers;

        -- Produtos: TODOS os 314 ficam, inclusive os 138 nunca vendidos (isso é insight)
        INSERT INTO gold.dim_product (item_id, category, category_label, order_lines, lines_rank)
        SELECT
            i.item_id,
            i.category,
            CASE i.category WHEN 'beer'   THEN 'Beer'
                            WHEN 'nab'    THEN 'Non-alcoholic'
                            WHEN 'liquor' THEN 'Spirits'
                            WHEN 'soda'   THEN 'Soft drinks'
                            ELSE 'Unclassified' END,
            COALESCE(l.n_lines, 0),
            ROW_NUMBER() OVER (ORDER BY COALESCE(l.n_lines, 0) DESC, i.item_id ASC)
        FROM lh_aurum_silver.dbo.silver_items AS i
        LEFT JOIN (
            SELECT item_id, COUNT(*) AS n_lines
            FROM lh_aurum_silver.dbo.silver_order_items
            GROUP BY item_id
        ) AS l ON l.item_id = i.item_id;

        -- 3) fact_orders: 1 linha por pedido + faixas e flags de análise
        INSERT INTO gold.fact_orders
            (order_id, date_key, user_id, revenue, item_count, basket_size, basket_size_order, is_fulfilled, is_outlier)
        SELECT
            o.order_id,
            YEAR(o.order_date) * 10000 + MONTH(o.order_date) * 100 + DAY(o.order_date),
            o.user_id,
            o.revenue,
            o.item_count,
            CASE WHEN o.item_count = 1  THEN '1'
                 WHEN o.item_count <= 3  THEN '2-3'
                 WHEN o.item_count <= 5  THEN '4-5'
                 WHEN o.item_count <= 10 THEN '6-10'
                 ELSE '11+' END,
            CASE WHEN o.item_count = 1  THEN 1
                 WHEN o.item_count <= 3  THEN 2
                 WHEN o.item_count <= 5  THEN 3
                 WHEN o.item_count <= 10 THEN 4
                 ELSE 5 END,
            CASE WHEN o.revenue > 0 THEN 1 ELSE 0 END,
            CASE WHEN o.revenue > s.avg_rev + 3 * s.std_rev THEN 1 ELSE 0 END
        FROM lh_aurum_silver.dbo.silver_orders AS o
        CROSS JOIN (
            SELECT AVG(CAST(revenue AS FLOAT))   AS avg_rev,
                   STDEV(CAST(revenue AS FLOAT)) AS std_rev
            FROM lh_aurum_silver.dbo.silver_orders
        ) AS s;

        -- 4) fact_order_items: produto + data + cliente + receita rateada (estimativa)
        INSERT INTO gold.fact_order_items (order_id, item_id, date_key, user_id, allocated_revenue_est)
        SELECT
            oi.order_id,
            oi.item_id,
            YEAR(o.order_date) * 10000 + MONTH(o.order_date) * 100 + DAY(o.order_date),
            o.user_id,
            CAST(o.revenue AS DECIMAL(18,6)) / o.item_count
        FROM lh_aurum_silver.dbo.silver_order_items AS oi
        JOIN lh_aurum_silver.dbo.silver_orders      AS o ON o.order_id = oi.order_id;

        -- 5) fact_targets_monthly: cada cliente x cada mês do calendário
        INSERT INTO gold.fact_targets_monthly (user_id, month_date_key, monthly_target, is_partial_month, has_orders)
        SELECT
            t.user_id,
            m.date_key,
            t.monthly_target,
            m.is_partial_month,
            CASE WHEN a.user_id IS NULL THEN 0 ELSE 1 END
        FROM lh_aurum_silver.dbo.silver_targets AS t
        CROSS JOIN (
            SELECT date_key, month_start, is_partial_month
            FROM lh_aurum_silver.dbo.silver_calendar
            WHERE [date] = month_start           -- só o 1º dia de cada mês
        ) AS m
        LEFT JOIN (                              -- meses em que cada cliente fez pelo menos 1 pedido
            SELECT DISTINCT user_id,
                   DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS month_start
            FROM lh_aurum_silver.dbo.silver_orders
        ) AS a
            ON a.user_id = t.user_id
           AND a.month_start = m.month_start;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;   -- repassa o erro: o pipeline marca a atividade como falha
    END CATCH;

    -- Resumo da carga (aparece na saída da atividade do pipeline)
    SELECT 'dim_date' AS tabela, COUNT(*) AS linhas FROM gold.dim_date
    UNION ALL SELECT 'dim_customer',         COUNT(*) FROM gold.dim_customer
    UNION ALL SELECT 'dim_product',          COUNT(*) FROM gold.dim_product
    UNION ALL SELECT 'fact_orders',          COUNT(*) FROM gold.fact_orders
    UNION ALL SELECT 'fact_order_items',     COUNT(*) FROM gold.fact_order_items
    UNION ALL SELECT 'fact_targets_monthly', COUNT(*) FROM gold.fact_targets_monthly;
END;

GO