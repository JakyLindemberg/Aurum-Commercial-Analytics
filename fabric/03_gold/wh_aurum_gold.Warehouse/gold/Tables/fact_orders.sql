CREATE TABLE [gold].[fact_orders] (
    [order_id]          BIGINT          NOT NULL,
    [date_key]          INT             NOT NULL,
    [user_id]           BIGINT          NOT NULL,
    [revenue]           DECIMAL (18, 2) NOT NULL,
    [item_count]        INT             NOT NULL,
    [basket_size]       VARCHAR (5)     NOT NULL,
    [basket_size_order] INT             NOT NULL,
    [is_fulfilled]      BIT             NOT NULL,
    [is_outlier]        BIT             NOT NULL
);


GO