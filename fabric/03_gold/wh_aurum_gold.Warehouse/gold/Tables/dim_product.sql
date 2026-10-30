CREATE TABLE [gold].[dim_product] (
    [item_id]        BIGINT       NOT NULL,
    [category]       VARCHAR (20) NOT NULL,
    [category_label] VARCHAR (30) NOT NULL,
    [order_lines]    INT          NOT NULL,
    [lines_rank]     INT          NOT NULL
);


GO