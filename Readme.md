# Proyecto Globant
## Presentación.
Mi nombre es Facundo Duran, soy Data Engineer y he pasado por varios proyectos de ingeniería de datos. A la hora de enfrentar los requerimientos que me solicitaron, decidí enforcarme en generar un script sencillo, en el cual utilicé herramientas con las que estoy familiarizado, para generar una solución lo mas genuina posible. Utilicé Pyspark para leer y escribir, Flask para levantar la API de manera sencilla. Generé en mi maquina una base de datos local de postgres. Utilicé testeos automatizados, los cuales pasaron las pruebas. Llevar adelante el proceso de la manera mas natural y real posible, hice pruebas con sqlite y cuando tuve el funcionamiento, pasé a generar una base postgres. Busqué claridad con los commits y utilizar el repositorio de manera apropiada para un proyecto de este tipo de una sola persona.
```
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
```
![image](https://user-images.githubusercontent.com/70112589/233371993-119a161c-64b3-471b-8b58-0018d9b75133.png)
```
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
```
![image](https://user-images.githubusercontent.com/70112589/233372282-6d3f597a-ee03-4b8d-a5d5-65d997ba0655.png)
