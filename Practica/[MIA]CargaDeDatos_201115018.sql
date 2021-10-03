--------------------------------------------------------Cargando información a la base de datos


--Cargar paises

WITH 
	solo_paises AS (
		select pais_cliente from datos 
		WHERE pais_cliente != '-'
		AND pais_cliente != ' '
		UNION
		select pais_empleado from datos
		WHERE pais_empleado != '-'
		AND pais_empleado != ' '
	)
INSERT INTO PAIS (nombre_pais) (select pais_cliente from solo_paises);


--Cargar categorias

WITH
	solo_categorias AS (
		select categoria_pelicula from datos
		WHERE categoria_pelicula != '-'
		AND categoria_pelicula != ' '
		GROUP BY categoria_pelicula
	)
INSERT INTO CATEGORIA(nombre_categoria) (select categoria_pelicula from solo_categorias);


--Cargar idiomas

WITH
	solo_idiomas AS (
		select lenguaje_pelicula from datos
		WHERE lenguaje_pelicula != '-'
		AND lenguaje_pelicula != ' '
		GROUP BY lenguaje_pelicula
	)
INSERT INTO IDIOMA(nombre_idioma) (select lenguaje_pelicula from solo_idiomas);



--Cargar actores

WITH
	solo_actores AS (
		select actor_pelicula from datos
		WHERE actor_pelicula != '-'
		AND actor_pelicula != ' '
		GROUP BY actor_pelicula
	)
INSERT INTO ACTOR (nombre_actor,apellido_actor) 
		(
			select  
			split_part (actor_pelicula, ' ',1) as Nombre, 
			split_part (actor_pelicula, ' ',2) as Apellido
			from solo_actores GROUP BY actor_pelicula
		);


--Cargar clasificaciones

WITH
	solo_clasificacion AS (
		select clasificacion from datos
		WHERE clasificacion != '-'
		AND clasificacion != ' '
		GROUP BY clasificacion
	)
INSERT INTO CLASIFICACION(nombre_clasificacion) (select clasificacion from solo_clasificacion);



--Cargar Ciudades

WITH
	datos_ciudad AS (
		select re_union.ciudad_cliente as Ciudad, re_union.pais_cliente as Pais from
		(select 	ciudad_cliente, pais_cliente from datos
			WHERE ciudad_cliente != '-'
			AND ciudad_cliente != ' '
			AND pais_cliente != '-'
			AND pais_cliente != ' '
			AND direccion_cliente != '-'
			AND direccion_cliente != ' '					
			UNION	
				(select ciudad_empleado, pais_empleado from datos
					WHERE ciudad_empleado != '-'
					AND ciudad_empleado != ' '
					AND pais_empleado != '-'
					AND pais_empleado != ' '
					AND direccion_empleado != '-'
					AND direccion_empleado != ' '
					UNION
					select 	ciudad_tienda, pais_tienda from datos
					WHERE ciudad_tienda != '-'
					AND ciudad_tienda != ' '
					AND pais_tienda != '-'
					AND pais_tienda != ' '
					AND direccion_tienda != '-'
					AND direccion_tienda != ' '
				ORDER BY ciudad_empleado ASC)
		) as re_union
	)
INSERT INTO CIUDAD(nombre_ciudad,fk_id_pais) 
	(
		select datos_ciudad.ciudad, PAIS.id_pais
			FROM datos_ciudad
			FULL OUTER JOIN PAIS
			ON datos_ciudad.pais = PAIS.nombre_pais
	);


--Cargar Peliculas

INSERT INTO PELICULA (titulo,descripcion,ano_lanzamiento,duracion,dias_disponibles_renta,costo_renta,costo_mal_estado,fk_id_clasificacion,fk_id_idioma)
(
	select d.nombre_pelicula,d.descripcion_pelicula,CAST(d.ano_lanzamiento AS INTEGER),CAST(d.duracion AS INTEGER),
			CAST(d.dias_renta AS INTEGER),TO_NUMBER(d.costo_renta,'9.99'),TO_NUMBER(d.costo_por_dano,'99.99'),cl.id_clasificacion, d.id_idioma from
		(
			select d.nombre_pelicula,d.descripcion_pelicula,d.ano_lanzamiento,d.duracion,d.dias_renta,d.costo_renta,
				d.costo_por_dano,d.clasificacion, d.lenguaje_pelicula, id.id_idioma from datos as d
				FULL OUTER JOIN 
				(
					select id_idioma, nombre_idioma from IDIOMA
				) as id
				ON id.nombre_idioma = d.lenguaje_pelicula
				WHERE d.nombre_pelicula != '-'
				AND d.nombre_pelicula != ' '
				AND d.descripcion_pelicula != '-'
				AND d.descripcion_pelicula != ' '
				AND d.ano_lanzamiento != '-'
				AND d.ano_lanzamiento != ' '
				AND d.duracion != '-'
				AND d.duracion != ' '
				AND d.dias_renta != '-'
				AND d.dias_renta != ' '
				AND d.costo_renta != '-'
				AND d.costo_renta != ' '
				AND d.costo_por_dano != '-'
				AND d.costo_por_dano != ' '
				GROUP BY d.nombre_pelicula,d.descripcion_pelicula,d.ano_lanzamiento,d.duracion,d.dias_renta,d.costo_renta,
				d.costo_por_dano,d.clasificacion, d.lenguaje_pelicula, id.id_idioma
		) AS d
		FULL OUTER JOIN (
			select nombre_clasificacion, id_clasificacion from CLASIFICACION
		) as cl
		ON cl.nombre_clasificacion = d.clasificacion
		ORDER BY d.nombre_pelicula ASC
);




--Cargar elenco de las películas


INSERT INTO ELENCO (fk_id_actor,fk_id_pelicula)
	(
		SELECT ta.id_actor,a.id_pelicula FROM
			(
			SELECT id_actor, nombre_actor || ' ' || apellido_actor AS nombre_actor FROM actor
			) AS ta
			FULL OUTER JOIN
			(SELECT a.actor_pelicula, a.nombre_pelicula,p.id_pelicula,p.titulo FROM 
				(
					SELECT actor_pelicula, nombre_pelicula FROM datos
					WHERE nombre_pelicula != '-' 
					AND actor_pelicula != '-'
					GROUP BY actor_pelicula, nombre_pelicula 
					ORDER BY nombre_pelicula
				) AS a
				FULL OUTER JOIN 
				(
					select id_pelicula, titulo FROM pelicula
				) AS p
				ON p.titulo = a.nombre_pelicula
			) as a
		ON a.actor_pelicula = ta.nombre_actor
		WHERE a.actor_pelicula != ' '
		AND a.nombre_pelicula != ' '
		ORDER BY a.nombre_pelicula
	);




--Llenar direcciones

INSERT INTO DIRECCION (direccion_detalle,codigo_postal,fk_id_ciudad)
	(
		SELECT a.direccion, a.codigo_postal, b.id_ciudad FROM
			(
				SELECT a.direccion_cliente as direccion, a.codigo_postal_cliente as codigo_postal, a.ciudad_cliente as ciudad FROM  
				(	
					SELECT direccion_cliente,codigo_postal_cliente,ciudad_cliente FROM  datos
					WHERE direccion_cliente != '-'
					AND direccion_cliente != ' '
					AND codigo_postal_cliente != '-'
					AND codigo_postal_cliente != ' '
					AND ciudad_cliente != '-'
					AND ciudad_cliente != ' '
					GROUP BY direccion_cliente,codigo_postal_cliente,ciudad_cliente
				) AS a
				UNION
				SELECT b.direccion_empleado, b.codigo_postal_empleado, b.ciudad_empleado FROM
				(	
					SELECT direccion_empleado,codigo_postal_empleado,ciudad_empleado FROM datos
					WHERE direccion_empleado != '-'
					AND direccion_empleado != ' '
					--AND codigo_postal_empleado != '-'
					AND codigo_postal_empleado != ' '
					AND ciudad_empleado != '-'
					AND ciudad_empleado != ' '
					GROUP BY direccion_empleado, codigo_postal_empleado,ciudad_empleado
				) AS b
				UNION
				SELECT c.direccion_tienda, c.codigo_postal_tienda, c.ciudad_tienda FROM
				(	
					SELECT direccion_tienda,codigo_postal_tienda,ciudad_tienda FROM datos
					WHERE direccion_tienda != '-'
					AND direccion_tienda != ' '
					--AND codigo_postal_tienda != '-'
					AND codigo_postal_tienda != ' '
					AND ciudad_tienda != '-'
					AND ciudad_tienda != ' '
					GROUP BY direccion_tienda, codigo_postal_tienda,ciudad_tienda
				) AS c	
			) AS a
			FULL OUTER JOIN
			(
				SELECT id_ciudad, nombre_ciudad FROM CIUDAD
			) AS b
		ON a.ciudad = b.nombre_ciudad
	)
;


--Cargar detalle categoría de películas

INSERT INTO DETALLE_CATEGORIA (fk_id_pelicula,fk_id_categoria)
	(
		SELECT a.id_pelicula,c.id_categoria FROM
			(
				SELECT a.nombre_pelicula,a.titulo,a.id_pelicula,a.categoria_pelicula FROM
					(
						SELECT d.nombre_pelicula, p.titulo, p.id_pelicula, d.categoria_pelicula FROM datos AS d
						FULL OUTER JOIN
						PELICULA AS p
						ON d.nombre_pelicula = p.titulo
						GROUP BY d.nombre_pelicula, p.titulo, p.id_pelicula, d.categoria_pelicula
					) AS a
					WHERE a.nombre_pelicula != '-'
					AND a.nombre_pelicula != ' '		
			) AS a	
		FULL OUTER JOIN CATEGORIA as c
		ON c.nombre_categoria = a.categoria_pelicula
	)
;


--Cargar traducciones

INSERT INTO TRADUCCION (fk_id_pelicula,fk_id_idioma)
	(
		SELECT a.id_pelicula,c.id_idioma FROM
			(
				SELECT a.nombre_pelicula,a.titulo,a.id_pelicula,a.lenguaje_pelicula FROM
					(
						SELECT d.nombre_pelicula, p.titulo, p.id_pelicula, d.lenguaje_pelicula FROM datos AS d
						FULL OUTER JOIN
						PELICULA AS p
						ON d.nombre_pelicula = p.titulo
						GROUP BY d.nombre_pelicula, p.titulo, p.id_pelicula, d.lenguaje_pelicula
					) AS a
					WHERE a.nombre_pelicula != '-'
					AND a.nombre_pelicula != ' '		
			) AS a	
		FULL OUTER JOIN IDIOMA as c
		ON c.nombre_idioma = a.lenguaje_pelicula
		WHERE a.id_pelicula IS NOT NULL
		ORDER BY a.id_pelicula
	)
;


--Cargar tienda
INSERT INTO TIENDA (nombre_tienda,encargado,fk_id_direccion)	
	(
		SELECT a.tienda,a.encargado,d.id_direccion FROM
			(
				SELECT nombre_tienda AS tienda, encargado_tienda AS encargado, direccion_tienda FROM datos 	
					WHERE nombre_tienda != '-'
					AND nombre_tienda != ''
					AND encargado_tienda != '-'
					AND encargado_tienda != ''
					AND direccion_tienda != '-'
					AND direccion_tienda != ''
					GROUP BY nombre_tienda, encargado_tienda, direccion_tienda
			) AS a
			FULL OUTER JOIN DIRECCION as d
			ON d.direccion_detalle = a.direccion_tienda
			WHERE a.tienda IS NOT NULL
			AND a.encargado IS NOT NULL
			AND a.direccion_tienda IS NOT NULL
	)
;


--Cargar empleado

INSERT INTO EMPLEADO (nombre_empleado,apellido_empleado,correo_empleado,activo_empleado,usuario,contrasena,fk_id_direccion,fk_id_tienda)
	(
		SELECT split_part(a.nombre_empleado, ' ',1) as Nombre,split_part(a.nombre_empleado, ' ',2) as Apellido, 
				a.correo_empleado, a.empleado_activo, a.usuario_empleado,a.contrasena_empleado,
				d.id_direccion, a.id_tienda FROM
			(
				SELECT a.nombre_empleado, a.correo_empleado, a.empleado_activo, a.tienda_empleado, 
					a.usuario_empleado, a.contrasena_empleado, a.direccion_empleado, t.id_tienda, t.nombre_tienda FROM
					(
						SELECT nombre_empleado, correo_empleado, empleado_activo, tienda_empleado, 
							usuario_empleado, contrasena_empleado, direccion_empleado from datos
							WHERE nombre_empleado != '-'
							AND nombre_empleado != ' ' 
							AND correo_empleado != '-'
							AND correo_empleado != ' ' 
							AND empleado_activo != '-'
							AND empleado_activo != ' ' 
							AND tienda_empleado != '-'
							AND tienda_empleado != ' ' 
							AND usuario_empleado != '-'
							AND usuario_empleado != ' ' 
							AND contrasena_empleado != '-'
							AND contrasena_empleado != ' ' 
							AND direccion_empleado != '-'
							AND direccion_empleado != ' ' 
							GROUP BY nombre_empleado, correo_empleado, empleado_activo, tienda_empleado, 
							usuario_empleado, contrasena_empleado, direccion_empleado
					) AS a
					FULL OUTER JOIN TIENDA as t
					ON a.tienda_empleado = t.nombre_tienda
			) AS a FULL OUTER JOIN DIRECCION as d
		ON a.direccion_empleado = d.direccion_detalle
		WHERE a.nombre_empleado IS NOT NULL
	)
;


--Cargar cliente

INSERT INTO CLIENTE (nombre_cliente,apellido_cliente,correo_cliente,fecha_registro,activo_cliente,fk_id_direccion,fk_id_tienda_favorita)
	(
		SELECT split_part(a.nombre_cliente, ' ',1) as Nombre, split_part(a.nombre_cliente, ' ',2) as Apellido, 
		a.correo_cliente, TO_DATE(a.fecha_creacion,'DD/MM/YYYY')as fecha_registro, a.cliente_activo, d.id_direccion, a.id_tienda FROM
			(
				SELECT a.nombre_cliente, a.correo_cliente, a.fecha_creacion, a.cliente_activo, a.tienda_preferida, a.direccion_cliente,
				t.nombre_tienda, t.id_tienda FROM
					(
						SELECT nombre_cliente, correo_cliente, fecha_creacion, cliente_activo ,tienda_preferida, direccion_cliente from datos
							WHERE nombre_cliente != '-'
							AND nombre_cliente != ' ' 
							AND correo_cliente != '-'
							AND correo_cliente != ' ' 
							AND fecha_creacion != '-'
							AND fecha_creacion != ' ' 
							AND cliente_activo != '-'
							AND cliente_activo != ' ' 
							AND tienda_preferida != '-'
							AND tienda_preferida != ' ' 
							AND direccion_cliente != '-'
							AND direccion_cliente != ' '
							GROUP BY nombre_cliente, correo_cliente, fecha_creacion, cliente_activo,tienda_preferida, direccion_cliente
					) AS a
					FULL OUTER JOIN TIENDA as t
					ON a.tienda_preferida = t.nombre_tienda
					ORDER BY a.nombre_cliente
			) AS a
		FULL OUTER JOIN DIRECCION as d
		ON a.direccion_cliente = d.direccion_detalle
		WHERE nombre_cliente IS NOT NULL
	)	
;



-- Cargar inventario
INSERT INTO INVENTARIO (fk_id_pelicula,fk_id_tienda)
	(
		SELECT a.id_pelicula,t.id_tienda FROM
			(
				SELECT a.nombre_pelicula,a.tienda_pelicula, p.id_pelicula, p.titulo FROM
					(
						SELECT a.nombre_pelicula,a.tienda_pelicula,TO_TIMESTAMP(fecha_renta, 'DD/MM/YYYY HH24:MM') as fecha_renta,
						TO_TIMESTAMP(fecha_retorno, 'DD/MM/YYYY HH24:MM') as fecha_retorno FROM datos as a
						WHERE tienda_pelicula != '-'
						AND fecha_retorno != '-'
						GROUP BY nombre_pelicula,tienda_pelicula,fecha_renta,fecha_retorno
					) AS a
				FULL OUTER JOIN PELICULA as p
				ON a.nombre_pelicula = p.titulo
				WHERE a.tienda_pelicula IS NOT NULL
				ORDER BY a.nombre_pelicula
			) AS a
		FULL OUTER JOIN TIENDA as t
		ON t.nombre_tienda = a.tienda_pelicula
		ORDER BY a.nombre_pelicula
	)
;



--Cargar Rentas

INSERT INTO RENTA (cantidad_pagar,fecha_pago,fecha_renta,fecha_devolucion,fk_id_empleado,fk_id_tienda,fk_id_cliente,fk_id_pelicula)
	(
		SELECT TO_NUMBER(a.monto_a_pagar,'99.99'), a.fecha_pago, a.fecha_renta, a.fecha_retorno, 
			a.id_empleado, a.id_tienda, a.id_cliente, p.id_pelicula FROM
			(
				SELECT a.monto_a_pagar, a.fecha_pago, a.fecha_renta, a.fecha_retorno, a.nombre_pelicula, 
					a.nombre_tienda, a.nombre_cliente, a.nombre_empleado, a.id_empleado, a.id_tienda, c.id_cliente FROM
					(
						SELECT a.monto_a_pagar, a.fecha_pago, a.fecha_renta, a.fecha_retorno, a.nombre_pelicula, 
								a.nombre_tienda, a.nombre_cliente, a.nombre_empleado, a.id_empleado, t.id_tienda FROM
							(	
								SELECT a.monto_a_pagar, a.fecha_pago, a.fecha_renta, a.fecha_retorno, a.nombre_pelicula, 
									a.nombre_tienda, a.nombre_cliente, a.nombre_empleado, e.id_empleado FROM
									(
										SELECT monto_a_pagar, fecha_pago, fecha_renta, fecha_retorno, nombre_pelicula, 
										nombre_tienda, nombre_cliente, nombre_empleado FROM datos
										WHERE monto_a_pagar != '-'
										AND monto_a_pagar != ' '
										AND fecha_pago != '-'
										AND fecha_pago != ' '
										AND fecha_renta != '-'
										AND fecha_renta != ' '
										AND nombre_pelicula != '-'
										AND nombre_pelicula != ' '
										AND nombre_tienda != '-'
										AND nombre_tienda != ' '
										AND nombre_cliente != '-'
										AND nombre_cliente != ' '
										AND nombre_empleado != '-'
										AND nombre_empleado != ' '
										GROUP BY monto_a_pagar, fecha_pago, fecha_renta, fecha_retorno, nombre_pelicula, 
										nombre_tienda, nombre_cliente, nombre_empleado
										ORDER BY nombre_cliente ASC
									) AS a
									FULL OUTER JOIN 
									(
										--Concatenar nombre y apellido del empleado para el join
										SELECT nombre_empleado || ' ' || apellido_empleado as nombre_empleado, id_empleado from EMPLEADO
										
									)AS e
								ON a.nombre_empleado = e.nombre_empleado
							)AS a
						FULL OUTER JOIN TIENDA as t
						ON a.nombre_tienda = t.nombre_tienda
					) AS a
				FULL OUTER JOIN 
					(
						--Concatenar nombre y apellido del cliente para el join
						SELECT nombre_cliente || ' ' || apellido_cliente as nombre_cliente, id_cliente from CLIENTE		
					) AS c
				ON a.nombre_cliente = c.nombre_cliente
			) AS a
		FULL OUTER JOIN PELICULA as p
		ON p.titulo = a.nombre_pelicula
		WHERE a.fecha_pago IS NOT NULL
		AND a.fecha_retorno IS NOT NULL
		AND a.fecha_retorno != ' '
	)
;