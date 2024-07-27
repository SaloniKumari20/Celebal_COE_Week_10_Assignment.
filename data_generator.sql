SELECT
    Risk,
    COUNT(*) AS Count
INTO
    PowerBIOutput
FROM
    InputEventHub
GROUP BY
    Risk,
    TumblingWindow(second, 10)
