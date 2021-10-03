--Count en la consulta 5


--Utilizo el count para contar la cantidad de rentas que tiene un cliente y la cantidad de rentas del país al que pertenece el cliente

WITH
consulta5 AS
	(
		SELECT a.nombre || ' ' || a.apellido as nombre, CAST(a.conteo AS INTEGER) as no_peliculas,
			p.nombre_pais, p.id_pais FROM
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
	)
	SELECT nombre, no_peliculas, CAST(no_peliculas AS FLOAT4)/
		(
		
			SELECT SUM(conteo) FROM
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
				WHERE id_pais = 
				(
					--Conseguir el id del pais a donde pertenece la que tiene más rentas para luego conseguir el total de personas de ese país
					SELECT id_pais FROM
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
				
				)
			GROUP BY conteo
			ORDER BY a.conteo DESC		
	
		)*100 as porcentaje, nombre_pais FROM consulta5	
;



--Count en la consulta 6

--Consulta utilizada para contar la cantidad de clientes por pais y luego la cantidad de clientes por cada ciudad

	SELECT COUNT(c.id_ciudad) as clientes_por_ciudad, ROUND(CAST(COUNT(c.id_ciudad) AS NUMERIC)/CAST(e.conteo AS NUMERIC),2)::FLOAT4 as porcentaje,  
	c.nombre_ciudad as ciudad,  d.nombre_pais as pais from CLIENTE as a
	INNER JOIN DIRECCION as b
	ON a.fk_id_direccion = b.id_direccion
	INNER JOIN CIUDAD as c
	ON b.fk_id_ciudad = c.id_ciudad
	INNER JOIN PAIS as d
	ON c.fk_id_pais = d.id_pais
	INNER JOIN 
	(
		SELECT d.id_pais, d.nombre_pais, COUNT(d.nombre_pais) as conteo from CLIENTE as a
		INNER JOIN DIRECCION as b
		ON a.fk_id_direccion = b.id_direccion
		INNER JOIN CIUDAD as c
		ON b.fk_id_ciudad = c.id_ciudad
		INNER JOIN PAIS as d
		ON c.fk_id_pais = d.id_pais
		GROUP BY d.nombre_pais,d.id_pais
		ORDER BY d.nombre_pais
	
	) as e
	ON d.id_pais = e.id_pais
	GROUP BY c.id_ciudad, c.nombre_ciudad, d.id_pais, d.nombre_pais, e.conteo
	ORDER BY d.nombre_pais
	;
	
		
		
--Count en la consulta 7

--Hace lo mismo que hace en la consulta 6. También utilizó otro para contar la cantidad de rentas por ciudad y por pais

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
		
		
		
--Count en la consulta 8

--Utilizó count para contar la cantidad de rentas por pais


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



--Count en la consulta 9

--Count para conteo de rentas por cliente,ciudad y pais

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



--Count en la consulta 10

--Count utilizado para contar la cantidad de rentas por categoría

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