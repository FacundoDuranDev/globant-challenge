SELECT d.department AS department, j.job AS job,
    COUNT(CASE WHEN EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2021 AND EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 1 THEN e.id END) AS Q1,
    COUNT(CASE WHEN EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2021 AND EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2 THEN e.id END) AS Q2,
    COUNT(CASE WHEN EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2021 AND EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 3 THEN e.id END) AS Q3,
    COUNT(CASE WHEN EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2021 AND EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 4 THEN e.id END) AS Q4
FROM employees e
JOIN departments d ON e.department_id::INTEGER = d.id
JOIN jobs j ON e.job_id = j.id
WHERE EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2021
GROUP BY d.department, j.job
ORDER BY d.department, j.job;
ERROR:  operator does not exist: character varying = integer



SELECT d.id, d.department AS department, COUNT(e.id) AS hired
FROM employees e
JOIN departments d ON e.department_id::INTEGER = d.id
WHERE EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2021
GROUP BY d.id, d.department
HAVING COUNT(e.id) > (
    SELECT AVG(hired)
    FROM (
        SELECT COUNT(e.id) AS hired
        FROM employees e
        JOIN departments d ON e.department_id::INTEGER = d.id
        WHERE EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS')) = 2021
        GROUP BY d.id
    ) AS department_counts
)
