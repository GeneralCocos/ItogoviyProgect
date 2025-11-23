CREATE SCHEMA IF NOT EXISTS cbr_dwh;

CREATE TABLE IF NOT EXISTS cbr_dwh.cbr_daily_rates_stage (
    rate_date  date        NOT NULL,
    num_code   varchar(3),
    char_code  varchar(3)  NOT NULL,
    nominal    integer     NOT NULL,
    name       text        NOT NULL,
    value      numeric(18,6) NOT NULL,
    load_dttm  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, char_code)
);
