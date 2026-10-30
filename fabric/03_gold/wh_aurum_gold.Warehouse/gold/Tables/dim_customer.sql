CREATE TABLE [gold].[dim_customer] (
    [user_id]        BIGINT        NOT NULL,
    [segment]        VARCHAR (50)  NULL,
    [city]           VARCHAR (100) NULL,
    [state]          VARCHAR (2)   NULL,
    [customer_label] VARCHAR (50)  NULL
);


GO