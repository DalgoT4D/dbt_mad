{% test validate_donation_amount(model) %}
SELECT
    *
FROM {{ model }}
WHERE donation_type IN ('ONE TIME', 'RECURRING')
  AND total_weekly_donations > 100000
{% endtest %}
