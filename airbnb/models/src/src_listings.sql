WITH raw_listings AS (
    select * from AIRBNB.RAW.RAW_LISTINGS
)

SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price As price_str,
    created_at,
    updated_at
FROM raw_listings