CREATE SCHEMA IF NOT EXISTS cbr;

CREATE TABLE IF NOT EXISTS cbr.cbr_daily_rates_raw (
    rate_date  date        NOT NULL,
    num_code   varchar(10) NOT NULL,
    char_code  varchar(3)  NOT NULL,
    nominal    integer     NOT NULL,
    name       text        NOT NULL,
    value      numeric(18,6) NOT NULL,
    load_dttm  timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT pk_cbr_daily_rates_raw PRIMARY KEY (rate_date, char_code)
);

CREATE INDEX IF NOT EXISTS idx_cbr_daily_rates_raw_char_code_date
    ON cbr.cbr_daily_rates_raw (char_code, rate_date);
