-- Please edit the sample below

CREATE MATERIALIZED VIEW silver_google_timeline AS
SELECT
    user_id,
    email,
    name,
    user_type
FROM samples.wanderbricks.users;