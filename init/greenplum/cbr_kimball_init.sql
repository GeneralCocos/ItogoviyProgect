CREATE SCHEMA IF NOT EXISTS cbr_dwh;

CREATE TABLE IF NOT EXISTS cbr_dwh.dim_date (
    date_key     integer      NOT NULL,     -- формат YYYYMMDD
    full_date    date         NOT NULL,
    year         integer      NOT NULL,
    month        integer      NOT NULL,
    day          integer      NOT NULL,
    month_name   varchar(20),
    day_of_week  integer,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
DISTRIBUTED BY (date_key);


CREATE TABLE IF NOT EXISTS cbr_dwh.dim_currency (
    currency_key  integer      NOT NULL,
    char_code     varchar(3)   NOT NULL,
    num_code      varchar(10),
    name          text         NOT NULL,
    is_active     boolean      NOT NULL,
    CONSTRAINT pk_dim_currency PRIMARY KEY (currency_key)
)
DISTRIBUTED BY (currency_key);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_currency_char_code
    ON cbr_dwh.dim_currency (char_code);


CREATE TABLE IF NOT EXISTS cbr_dwh.fact_exchange_rate (
    date_key      integer       NOT NULL,
    currency_key  integer       NOT NULL,
    rate_value    numeric(18,6) NOT NULL,
    nominal       integer       NOT NULL,
    load_dttm     timestamptz   NOT NULL DEFAULT now(),

    CONSTRAINT pk_fact_exchange_rate PRIMARY KEY (date_key, currency_key),

    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_key)
        REFERENCES cbr_dwh.dim_date (date_key),

    CONSTRAINT fk_fact_currency
        FOREIGN KEY (currency_key)
        REFERENCES cbr_dwh.dim_currency (currency_key)
)
DISTRIBUTED BY (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_currency_date
    ON cbr_dwh.fact_exchange_rate (currency_key, date_key);
