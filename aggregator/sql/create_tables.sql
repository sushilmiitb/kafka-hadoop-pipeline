CREATE TABLE IF NOT EXISTS ad_metrics (
    id              uuid,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    serves          BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    modified_on     timestamp DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)
);

CREATE TABLE IF NOT EXISTS adgroup_metrics (
    id              uuid,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    serves          BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    modified_on     timestamp DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)
);

CREATE TABLE IF NOT EXISTS advertiser_metrics (
    id              uuid,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    serves          BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    modified_on     timestamp DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)
);

CREATE TABLE IF NOT EXISTS app_metrics (
    id              uuid,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    requests        BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    modified_on     timestamp DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)
);

CREATE TABLE IF NOT EXISTS placement_metrics (
    id              uuid,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    requests        BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    modified_on     timestamp DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)
);

CREATE TABLE IF NOT EXISTS publisher_metrics (
    id              uuid,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    hour            SMALLINT,
    requests        BIGINT,
    impressions     BIGINT,
    clicks          BIGINT,
    errors          BIGINT,
    closes          BIGINT,
    modified_on     timestamp DEFAULT current_timestamp,
    PRIMARY KEY(id, year, month, day, hour)

);