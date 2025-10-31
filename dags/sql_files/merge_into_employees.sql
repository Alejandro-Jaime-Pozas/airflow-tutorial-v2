INSERT INTO employees
SELECT *
FROM (
    SELECT DISTINCT *
    FROM employees_temp
) t
ON CONFLICT ("Serial Number") DO UPDATE
SET
        "Employee Markme" = excluded."Employee Markme",
        "Description" = excluded."Description",
        "Leave" = excluded."Leave";
