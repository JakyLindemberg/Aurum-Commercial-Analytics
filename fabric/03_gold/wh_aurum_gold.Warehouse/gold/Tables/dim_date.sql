CREATE TABLE [gold].[dim_date] (
    [date_key]         INT          NOT NULL,
    [date]             DATE         NOT NULL,
    [year]             INT          NOT NULL,
    [month_num]        INT          NOT NULL,
    [month_name]       VARCHAR (3)  NOT NULL,
    [month_start]      DATE         NOT NULL,
    [day_of_week]      VARCHAR (10) NOT NULL,
    [weekday_num]      INT          NOT NULL,
    [is_partial_month] BIT          NOT NULL
);


GO