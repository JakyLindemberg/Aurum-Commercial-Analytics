CREATE TABLE [gold].[fact_order_items] (
    [order_id]              BIGINT          NOT NULL,
    [item_id]               BIGINT          NOT NULL,
    [date_key]              INT             NOT NULL,
    [user_id]               BIGINT          NOT NULL,
    [allocated_revenue_est] DECIMAL (18, 6) NOT NULL
);


GO