CREATE TABLE IF NOT EXISTS ad_metrics (
    id              TEXT,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    serves          BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    amount          DOUBLE PRECISION,
    modified_on     TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)
);

CREATE TABLE IF NOT EXISTS placement_metrics (
    id              TEXT,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    requests        BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    amount          DOUBLE PRECISION,
    modified_on     TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)
);