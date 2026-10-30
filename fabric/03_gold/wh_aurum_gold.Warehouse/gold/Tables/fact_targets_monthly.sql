CREATE TABLE [gold].[fact_targets_monthly] (
    [user_id]          BIGINT          NOT NULL,
    [month_date_key]   INT             NOT NULL,
    [monthly_target]   DECIMAL (18, 2) NOT NULL,
    [is_partial_month] BIT             NOT NULL,
    [has_orders]       BIT             NOT NULL
);


GO