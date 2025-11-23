CREATE SCHEMA IF NOT EXISTS cbr_dwh;

CREATE TABLE IF NOT EXISTS cbr_dwh.cbr_daily_rates_stage (
    rate_date  date        NOT NULL,
    num_code   varchar(10) NOT NULL,
    char_code  varchar(3)  NOT NULL,
    nominal    integer     NOT NULL,
    name       text        NOT NULL,
    value      numeric(18,6) NOT NULL,
    op_ts      timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_cbr_daily_rates_stage PRIMARY KEY (rate_date, char_code)
);

CREATE INDEX IF NOT EXISTS idx_cbr_stage_char_code_date
    ON cbr_dwh.cbr_daily_rates_stage (char_code, rate_date);


CREATE TABLE IF NOT EXISTS cbr_dwh.dim_currency (
    currency_key  bigserial    PRIMARY KEY,
    char_code     varchar(3)   NOT NULL,
    num_code      varchar(10),
    name          text         NOT NULL,
    is_active     boolean      NOT NULL DEFAULT true,
    valid_from    date         NOT NULL DEFAULT current_date,
    valid_to      date         NOT NULL DEFAULT '2999-12-31',

    CONSTRAINT uq_dim_currency_char_code_valid_to
        UNIQUE (char_code, valid_to)
);

CREATE INDEX IF NOT EXISTS idx_dim_currency_char_code
    ON cbr_dwh.dim_currency (char_code);


CREATE TABLE IF NOT EXISTS cbr_dwh.fact_exchange_rate (
    fact_id       bigserial     PRIMARY KEY,
    rate_date     date          NOT NULL,
    currency_key  bigint        NOT NULL
        REFERENCES cbr_dwh.dim_currency(currency_key),
    rate_value    numeric(18,6) NOT NULL,
    nominal       integer       NOT NULL,
    load_dttm     timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_rate_date
    ON cbr_dwh.fact_exchange_rate (rate_date);

CREATE INDEX IF NOT EXISTS idx_fact_currency_key
    ON cbr_dwh.fact_exchange_rate (currency_key);
