-- Direccion
-- \i '/home/joddie/Desktop/practica/Practica/consultas.sql'



/*
--Consulta 1

SELECT COUNT(id_inventario) AS Cantidad_inventario from INVENTARIO WHERE fk_id_pelicula = 
	(
		SELECT id_pelicula FROM PELICULA WHERE LOWER(titulo) = LOWER('Sugar Wonka')
	);


	
	
--Consulta 2

	
SELECT a.nombre_cliente as nombre, a.apellido_cliente as apellido,a.conteo, a.pagado  FROM
	(
		SELECT fk_id_cliente as id_cliente, COUNT(fk_id_cliente) as conteo, 
		c.nombre_cliente,c.apellido_cliente, SUM(cantidad_pagar) as pagado FROM renta as a
		FULL OUTER JOIN CLIENTE as c
		ON a.fk_id_cliente = c.id_cliente
		GROUP BY fk_id_cliente,c.nombre_cliente,c.apellido_cliente
		ORDER BY c.nombre_cliente
	)AS a
	WHERE conteo >= 40
;



--Consulta 3

SELECT nombre_actor || ' ' || apellido_actor as nombre_actor from ACTOR 
WHERE apellido_actor LIKE 'Son%'
OR apellido_actor LIKE '%son%'
ORDER BY nombre_actor;

--Consulta 4

WITH
	peliculas AS
		(
			SELECT id_pelicula,titulo,ano_lanzamiento FROM PELICULA
			WHERE LOWER(descripcion) LIKE '%crocodile%' 
			AND LOWER(descripcion) LIKE '%shark%'
		)	
	SELECT p.titulo as pelicula,a.nombre_actor,a.apellido_actor,p.ano_lanzamiento FROM peliculas as p
	INNER JOIN ELENCO as e
	ON e.fk_id_pelicula = p.id_pelicula
	INNER JOIN Actor as a
	ON a.id_actor = e.fk_id_actor
	ORDER BY a.apellido_actor ASC
;


--Consulta 5

SELECT a.nombre || ' ' || a.apellido as nombre, CAST(a.conteo AS INTEGER) as no_peliculas,
	CAST(a.conteo/(select COUNT(id_renta) FROM RENTA)AS NUMERIC(5,5)) as porcentaje, p.nombre_pais FROM
	(
		SELECT a.nombre_cliente as nombre, a.apellido_cliente as apellido,CAST(a.conteo AS NUMERIC(5,2)), c.fk_id_direccion AS direccion  FROM
			(
				SELECT fk_id_cliente as id_cliente, COUNT(fk_id_cliente) as conteo, 
				c.nombre_cliente,c.apellido_cliente FROM renta as a
				FULL OUTER JOIN CLIENTE as c
				ON a.fk_id_cliente = c.id_cliente
				GROUP BY fk_id_cliente,c.nombre_cliente,c.apellido_cliente
				ORDER BY c.nombre_cliente
			)AS a
			FULL OUTER JOIN CLIENTE as c
			ON c.nombre_cliente || ' ' || c.apellido_cliente = a.nombre_cliente || ' ' || a.apellido_cliente
	)AS a
	INNER JOIN DIRECCION as d
	ON a.direccion = d.id_direccion
	INNER JOIN CIUDAD as c
	ON d.fk_id_ciudad = c.id_ciudad
	INNER JOIN PAIS as p
	ON c.fk_id_pais = p.id_pais
	ORDER BY a.conteo DESC
	limit 1
;
	



--Consulta 6


WITH
	paises AS
	(
		SELECT a.nombre,a.apellido, a.conteo as no_peliculas,
			p.nombre_pais,c.nombre_ciudad, p.id_pais,c.id_ciudad FROM
			(
				SELECT a.nombre_cliente as nombre, a.apellido_cliente as apellido,a.conteo , c.fk_id_direccion AS direccion  FROM
					(
						SELECT fk_id_cliente as id_cliente, COUNT(fk_id_cliente) as conteo, 
						c.nombre_cliente,c.apellido_cliente FROM renta as a
						FULL OUTER JOIN CLIENTE as c
						ON a.fk_id_cliente = c.id_cliente
						GROUP BY fk_id_cliente,c.nombre_cliente,c.apellido_cliente
						ORDER BY c.nombre_cliente
					)AS a
					FULL OUTER JOIN CLIENTE as c
					ON c.id_cliente = a.id_cliente
			)AS a
			INNER JOIN DIRECCION as d
			ON a.direccion = d.id_direccion
			INNER JOIN CIUDAD as c
			ON d.fk_id_ciudad = c.id_ciudad
			INNER JOIN PAIS as p
			ON c.fk_id_pais = p.id_pais
			ORDER BY p.nombre_pais DESC
	)
		--Conteo por ciudad
		SELECT CAST((a.no_peliculas/totales_pais.no_pais)*100 AS NUMERIC(5,2)) as porcentaje_ciudad,a.nombre_ciudad,pais.nombre_pais FROM
		(
			SELECT SUM(no_peliculas) as no_peliculas,nombre_ciudad, id_ciudad FROM paises
			GROUP BY nombre_ciudad, id_ciudad
			ORDER BY nombre_ciudad
		) AS a
		INNER JOIN ciudad
		ON ciudad.id_ciudad = a.id_ciudad
		INNER JOIN pais
		ON pais.id_pais = ciudad.fk_id_pais
		INNER JOIN 
		(
			--Conteo por pais
			SELECT SUM(no_peliculas) as no_pais, nombre_pais, id_pais FROM paises
			GROUP BY nombre_pais, id_pais
			ORDER BY nombre_pais
		) as totales_pais
		ON totales_pais.id_pais = pais.id_pais
		ORDER BY pais.nombre_pais;
	
	
WITH
	paises AS
	(
		SELECT a.nombre,a.apellido, a.conteo as no_peliculas,
			p.nombre_pais,c.nombre_ciudad, p.id_pais,c.id_ciudad FROM
			(
				SELECT a.nombre_cliente as nombre, a.apellido_cliente as apellido,a.conteo , c.fk_id_direccion AS direccion  FROM
					(
						SELECT fk_id_cliente as id_cliente, COUNT(fk_id_cliente) as conteo, 
						c.nombre_cliente,c.apellido_cliente FROM renta as a
						FULL OUTER JOIN CLIENTE as c
						ON a.fk_id_cliente = c.id_cliente
						GROUP BY fk_id_cliente,c.nombre_cliente,c.apellido_cliente
						ORDER BY c.nombre_cliente
					)AS a
					FULL OUTER JOIN CLIENTE as c
					ON c.id_cliente = a.id_cliente
			)AS a
			INNER JOIN DIRECCION as d
			ON a.direccion = d.id_direccion
			INNER JOIN CIUDAD as c
			ON d.fk_id_ciudad = c.id_ciudad
			INNER JOIN PAIS as p
			ON c.fk_id_pais = p.id_pais
			ORDER BY p.nombre_pais DESC
	)				
			--Conteo por pais
		SELECT CAST ((a.no_pais/(SELECT COUNT(id_renta) FROM renta))*100 AS NUMERIC(5,2)) as porcentaje_pais,a.nombre_pais FROM
		(
			SELECT SUM(no_peliculas) as no_pais, nombre_pais, id_pais FROM paises
			GROUP BY nombre_pais, id_pais
			ORDER BY nombre_pais
		) AS a
		ORDER BY a.no_pais DESC;


--consulta 7

WITH
	paises AS
	(
		SELECT a.nombre,a.apellido, a.conteo as no_peliculas,
			p.nombre_pais,c.nombre_ciudad, p.id_pais,c.id_ciudad FROM
			(
				SELECT a.nombre_cliente as nombre, a.apellido_cliente as apellido,a.conteo , c.fk_id_direccion AS direccion  FROM
					(
						SELECT fk_id_cliente as id_cliente, COUNT(fk_id_cliente) as conteo, 
						c.nombre_cliente,c.apellido_cliente FROM renta as a
						FULL OUTER JOIN CLIENTE as c
						ON a.fk_id_cliente = c.id_cliente
						GROUP BY fk_id_cliente,c.nombre_cliente,c.apellido_cliente
						ORDER BY c.nombre_cliente
					)AS a
					FULL OUTER JOIN CLIENTE as c
					ON c.id_cliente = a.id_cliente
			)AS a
			INNER JOIN DIRECCION as d
			ON a.direccion = d.id_direccion
			INNER JOIN CIUDAD as c
			ON d.fk_id_ciudad = c.id_ciudad
			INNER JOIN PAIS as p
			ON c.fk_id_pais = p.id_pais
			ORDER BY p.nombre_pais DESC
	)
		--Conteo por ciudad
		SELECT a.nombre_ciudad as ciudad,a.nombre_pais as pais, 
		a.no_peliculas as rentas, CAST((CAST (a.no_peliculas AS NUMERIC(5,2))/CAST(b.conteo_ciudad AS NUMERIC(5,2))) AS NUMERIC(5,2)) as promedio
		FROM paises as a
		INNER JOIN 
		(
			--Conteo por pais
			SELECT COUNT(p.nombre_pais) as conteo_ciudad,p.nombre_pais,p.id_pais FROM CIUDAD as c
			INNER JOIN PAIS as p
			ON c.fk_id_pais = p.id_pais
			GROUP BY p.nombre_pais, p.id_pais
			ORDER BY p.nombre_pais				
		) as b
		ON b.id_pais = a.id_pais
		GROUP BY a.nombre_ciudad, a.nombre_pais, a.no_peliculas, b.conteo_ciudad
		ORDER BY a.nombre_pais ASC,a.no_peliculas DESC;



--Consulta 8

	--Filtrado de Sportas
	SELECT COUNT(e.nombre_pais) rentas, CAST(CAST(COUNT(e.nombre_pais) AS NUMERIC (6,2))/CAST(i.rentas  AS NUMERIC(6,2)) AS NUMERIC(6,2)) as porcentaje,
	e.nombre_pais as pais
	FROM renta as a
	INNER JOIN cliente as b	
	ON a.fk_id_cliente = b.id_cliente
	INNER JOIN direccion as c
	ON b.fk_id_direccion = c.id_direccion
	INNER JOIN ciudad as d
	ON c.fk_id_ciudad = d.id_ciudad
	INNER JOIN pais as e
	ON d.fk_id_pais = e.id_pais
	INNER JOIN pelicula as f
	ON a.fk_id_pelicula = f.id_pelicula
	INNER JOIN detalle_categoria as g
	ON f.id_pelicula = g.fk_id_pelicula
	INNER JOIN
	(
		--Filtrado de totales por pais
		SELECT COUNT(e.nombre_pais) rentas,e.nombre_pais, e.id_pais FROM renta as a
		INNER JOIN cliente as b	
		ON a.fk_id_cliente = b.id_cliente
		INNER JOIN direccion as c
		ON b.fk_id_direccion = c.id_direccion
		INNER JOIN ciudad as d
		ON c.fk_id_ciudad = d.id_ciudad
		INNER JOIN pais as e
		ON d.fk_id_pais = e.id_pais
		GROUP BY e.nombre_pais,e.id_pais
		ORDER BY e.nombre_pais
	) AS i
	ON e.id_pais = i.id_pais
	INNER JOIN categoria as h
	ON h.id_categoria = g.fk_id_categoria
	WHERE h.nombre_categoria = 'Sports'
	GROUP BY e.nombre_pais,i.nombre_pais,i.rentas
	ORDER BY e.nombre_pais
;




--Consulta 9

WITH
	paises AS
	(
		SELECT a.nombre,a.apellido, a.conteo as no_peliculas,
			p.nombre_pais,c.nombre_ciudad, p.id_pais,c.id_ciudad FROM
			(
				SELECT a.nombre_cliente as nombre, a.apellido_cliente as apellido,a.conteo , c.fk_id_direccion AS direccion  FROM
					(
						SELECT fk_id_cliente as id_cliente, COUNT(fk_id_cliente) as conteo, 
						c.nombre_cliente,c.apellido_cliente FROM renta as a
						FULL OUTER JOIN CLIENTE as c
						ON a.fk_id_cliente = c.id_cliente
						GROUP BY fk_id_cliente,c.nombre_cliente,c.apellido_cliente
						ORDER BY c.nombre_cliente
					)AS a
					FULL OUTER JOIN CLIENTE as c
					ON c.id_cliente = a.id_cliente
			)AS a
			INNER JOIN DIRECCION as d
			ON a.direccion = d.id_direccion
			INNER JOIN CIUDAD as c
			ON d.fk_id_ciudad = c.id_ciudad
			INNER JOIN PAIS as p
			ON c.fk_id_pais = p.id_pais
			ORDER BY p.nombre_pais DESC
	)
		--Conteo por ciudad
		SELECT a.nombre_ciudad as ciudad,a.nombre_pais as pais, a.no_peliculas as rentas
		FROM paises as a
		INNER JOIN 
		(
			--Conteo por pais
			SELECT COUNT(p.nombre_pais) as conteo_ciudad,p.nombre_pais,p.id_pais FROM CIUDAD as c
			INNER JOIN PAIS as p
			ON c.fk_id_pais = p.id_pais
			GROUP BY p.nombre_pais, p.id_pais
			ORDER BY p.nombre_pais				
		) as b
		ON b.id_pais = a.id_pais
		WHERE a.nombre_pais = 'United States'
		AND a.no_peliculas > 
			(
				SELECT a.rentas FROM 
					(
						--Conteo por ciudad
						SELECT a.nombre_ciudad as ciudad,a.nombre_pais as pais, a.no_peliculas as rentas
						FROM paises as a
						INNER JOIN 
						(
							--Conteo por pais
							SELECT COUNT(p.nombre_pais) as conteo_ciudad,p.nombre_pais,p.id_pais FROM CIUDAD as c
							INNER JOIN PAIS as p
							ON c.fk_id_pais = p.id_pais
							GROUP BY p.nombre_pais, p.id_pais
							ORDER BY p.nombre_pais				
						) as b
						ON b.id_pais = a.id_pais
						WHERE a.nombre_ciudad = 'Dayton' --Nombre de la ciudad
						GROUP BY a.nombre_ciudad, a.nombre_pais, a.no_peliculas, b.conteo_ciudad
						ORDER BY a.nombre_pais ASC,a.no_peliculas DESC
					) AS a
			)
		GROUP BY a.nombre_ciudad, a.nombre_pais, a.no_peliculas, b.conteo_ciudad
		ORDER BY a.nombre_pais ASC,a.no_peliculas DESC;	
*/

--Consulta 10

WITH
	tabla_resultado AS
	(
		SELECT *,ROW_NUMBER() OVER (PARTITION BY a.nombre_ciudad order by a.nombre_ciudad, a.contador DESC) AS fila_id FROM
			(
				SELECT d.nombre_ciudad,e.nombre_pais, h.nombre_categoria,
				COUNT (h.nombre_categoria) as contador
				FROM renta as a
				INNER JOIN CLIENTE as b
				ON a.fk_id_cliente = b.id_cliente
				INNER JOIN DIRECCION as c
				ON b.fk_id_direccion = c.id_direccion
				INNER JOIN CIUDAD as d
				ON c.fk_id_ciudad = d.id_ciudad
				INNER JOIN PAIS as e
				ON d.fk_id_pais = e.id_pais
				INNER JOIN PELICULA as f
				ON a.fk_id_pelicula = f.id_pelicula
				INNER JOIN DETALLE_CATEGORIA as g
				ON a.fk_id_pelicula = g.fk_id_pelicula
				INNER JOIN CATEGORIA as h
				ON g.fk_id_categoria = h.id_categoria
				GROUP BY d.nombre_ciudad,e.nombre_pais, h.nombre_categoria
				ORDER BY d.nombre_ciudad, contador DESC		
			) as a
	)
	SELECT nombre_ciudad as ciudad, nombre_pais as pais, nombre_categoria as categoria, contador as rentas from tabla_resultado
	WHERE fila_id = 1
	AND nombre_categoria = 'Horror'
	--WHERE nombre_pais = 'Angola'
	;
