WITH rev AS (
    SELECT * from {{ ref('fct_reviews') }}
),

list AS (
    SELECT * from {{ ref('dim_listings_cleansed') }}
)


SELECT rev.*, list.created_at as listing_created_at from rev
JOIN list ON rev.listing_id = list.listing_id
WHERE rev.review_date < list.created_at
LIMIT 10