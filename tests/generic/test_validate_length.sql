{% test validate_length(model) %}
WITH donation_duration AS (
    SELECT
        MIN(donation_week) AS start_date,
        MAX(donation_week) AS end_date,
        donation_type
    FROM {{ model }}
    WHERE donation_type = 'Recurring'
    GROUP BY donation_type
),
invalid_donations AS (
    SELECT
        *,
        AGE(end_date, start_date) AS donation_length,
        DATE_PART('year', AGE(end_date, start_date)) AS donation_length_years
    FROM donation_duration
    WHERE DATE_PART('year', AGE(end_date, start_date)) > 3
)
SELECT * FROM invalid_donations
{% endtest %}
