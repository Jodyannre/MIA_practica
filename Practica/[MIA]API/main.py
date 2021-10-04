from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import os
import json2table
import json

app = Flask(__name__)


#IMPLEMENTAR CORS PARA NO TENER ERRORES AL TRATAR ACCEDER AL SERVIDOR DESDE OTRO SERVER EN DIFERENTE LOCACIÓN
CORS(app)

DB_HOST = "localhost"
DB_NAME = "practica"
DB_USER = "joddie"
DB_PASS = "Huitzi12"


crearModelo = """
--Crear tablas

CREATE TABLE PAIS (
	id_pais SERIAL PRIMARY KEY,
	nombre_pais VARCHAR ( 50 ) NOT NULL
);

CREATE TABLE CATEGORIA (
	id_categoria SERIAL PRIMARY KEY,
	nombre_categoria VARCHAR ( 50 ) NOT NULL
);

CREATE TABLE IDIOMA (
	id_idioma SERIAL PRIMARY KEY,
	nombre_idioma VARCHAR ( 50 ) NOT NULL
);

CREATE TABLE CLASIFICACION (
	id_clasificacion SERIAL PRIMARY KEY,
	nombre_clasificacion VARCHAR ( 5 ) 
);

CREATE TABLE ACTOR (
	id_actor SERIAL PRIMARY KEY,
	nombre_actor VARCHAR ( 100 ) NOT NULL,
	apellido_actor VARCHAR ( 100 ) NOT NULL
);

CREATE TABLE CIUDAD (
	id_ciudad SERIAL PRIMARY KEY,
	nombre_ciudad VARCHAR ( 50 ) NOT NULL,
	fk_id_pais INT,
	FOREIGN KEY (fk_id_pais)
		REFERENCES PAIS(id_pais)
);

CREATE TABLE PELICULA(
	id_pelicula SERIAL PRIMARY KEY,
	titulo VARCHAR( 100 ) NOT NULL,
	descripcion VARCHAR ( 250 ) NOT NULL,
	ano_lanzamiento INT NOT NULL,
	duracion INT NOT NULL,
	dias_disponibles_renta INT NOT NULL,
	costo_renta NUMERIC(5,2) NOT NULL,
	costo_mal_estado NUMERIC(5,2) NOT NULL,
	fk_id_clasificacion INT NOT NULL,
	fk_id_idioma INT NOT NULL,
	FOREIGN KEY (fk_id_clasificacion)
		REFERENCES CLASIFICACION(id_clasificacion),
	FOREIGN KEY (fk_id_idioma)
		REFERENCES IDIOMA(id_idioma)
);

CREATE TABLE ELENCO (
	id_elenco SERIAL PRIMARY KEY,
	fk_id_actor INT NOT NULL,
	fk_id_pelicula INT NOT NULL,
	FOREIGN KEY (fk_id_actor)
		REFERENCES ACTOR(id_actor),
	FOREIGN KEY (fk_id_pelicula)
		REFERENCES PELICULA(id_pelicula)
);

CREATE TABLE DETALLE_CATEGORIA (
	id_detalle_categoria SERIAL PRIMARY KEY,
	fk_id_pelicula INT NOT NULL,
	fk_id_categoria INT NOT NULL,
	FOREIGN KEY (fk_id_pelicula)
		REFERENCES PELICULA(id_pelicula),
	FOREIGN KEY (fk_id_categoria)
		REFERENCES CATEGORIA(id_categoria)
);

CREATE TABLE TRADUCCION (
	id_traduccion SERIAL PRIMARY KEY,
	fk_id_pelicula INT NOT NULL,
	fk_id_idioma INT NOT NULL,
	FOREIGN KEY (fk_id_pelicula)
		REFERENCES PELICULA(id_pelicula),
	FOREIGN KEY (fk_id_idioma)
		REFERENCES IDIOMA(id_idioma)	
);

CREATE TABLE DIRECCION(
	id_direccion SERIAL PRIMARY KEY,
	direccion_detalle VARCHAR ( 150 ) NOT NULL,
	codigo_postal VARCHAR ( 10 ) NOT NULL,
	fk_id_ciudad INT NOT NULL,
	FOREIGN KEY (fk_id_ciudad)
		REFERENCES CIUDAD(id_ciudad)
);

CREATE TABLE TIENDA(
	id_tienda SERIAL PRIMARY KEY,
	nombre_tienda VARCHAR ( 100 ) NOT NULL,
	encargado VARCHAR ( 150 ) NOT NULL,
	fk_id_direccion INT NOT NULL,
	FOREIGN KEY (fk_id_direccion)
		REFERENCES DIRECCION(id_direccion)
);

CREATE TABLE INVENTARIO(
	id_inventario SERIAL PRIMARY KEY,
	fk_id_pelicula INT NOT NULL,
	fk_id_tienda INT NOT NULL,
	cantidad INT NOT NULL
);

CREATE TABLE EMPLEADO(
	id_empleado SERIAL PRIMARY KEY,
	nombre_empleado VARCHAR ( 150 ) NOT NULL,
	apellido_empleado VARCHAR ( 150 ) NOT NULL,
	correo_empleado VARCHAR ( 150 ) NOT NULL,
	activo_empleado VARCHAR ( 3 ) NOT NULL,
	usuario VARCHAR ( 150 ) NOT NULL,
	contrasena VARCHAR ( 150 ) NOT NULL,
	fk_id_direccion INT NOT NULL,
	fk_id_tienda INT NOT NULL,
	FOREIGN KEY (fk_id_direccion)
		REFERENCES DIRECCION(id_direccion),
	FOREIGN KEY (fk_id_tienda)
		REFERENCES TIENDA(id_tienda)		
);

CREATE TABLE CLIENTE(
	id_cliente SERIAL PRIMARY KEY,
	nombre_cliente VARCHAR ( 150 ) NOT NULL,
	apellido_cliente VARCHAR ( 150 ) NOT NULL,
	correo_cliente VARCHAR ( 150 ) NOT NULL,
	fecha_registro DATE NOT NULL,
	activo_cliente VARCHAR ( 3 ) NOT NULL,
	fk_id_direccion INT NOT NULL,
	fk_id_tienda_favorita INT NOT NULL,
	FOREIGN KEY (fk_id_direccion)
		REFERENCES DIRECCION(id_direccion),
	FOREIGN KEY (fk_id_tienda_favorita)
		REFERENCES TIENDA(id_tienda)		
);


CREATE TABLE RENTA(
	id_renta SERIAL PRIMARY KEY,
	cantidad_pagar NUMERIC(5,2) NOT NULL,
	fecha_pago VARCHAR ( 150 ) NOT NULL,
	fecha_renta VARCHAR ( 150 ) NOT NULL,
	fecha_devolucion VARCHAR ( 150 ) NOT NULL,
	fk_id_empleado INT NOT NULL,
	fk_id_tienda INT NOT NULL,
	fk_id_cliente INT NOT NULL,
	fk_id_pelicula INT NOT NULL,
	FOREIGN KEY (fk_id_empleado)
		REFERENCES EMPLEADO(id_empleado),
	FOREIGN KEY (fk_id_tienda)
		REFERENCES TIENDA(id_tienda),
	FOREIGN KEY (fk_id_cliente)
		REFERENCES CLIENTE(id_cliente),
	FOREIGN KEY (fk_id_pelicula)
		REFERENCES PELICULA(id_pelicula)
);"""

cargarModelo = """--Cargar paises

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
INSERT INTO INVENTARIO (fk_id_pelicula,fk_id_tienda, cantidad)
	(
		SELECT a.id_pelicula, a.id_tienda, COUNT(a.nombre_pelicula) as inventario FROM
			(
				SELECT a.nombre_pelicula,a.nombre_tienda, a.nombre_cliente,a.ano_lanzamiento, p.id_pelicula, t.id_tienda FROM datos AS a
				INNER JOIN CLIENTE as c
				ON a.nombre_cliente = c.nombre_cliente || ' ' || c.apellido_cliente
				INNER JOIN TIENDA as t
				ON t.nombre_tienda = a.nombre_tienda
				INNER JOIN PELICULA as p
				ON p.titulo = a.nombre_pelicula
				AND p.ano_lanzamiento = TO_NUMBER(a.ano_lanzamiento,'9999')
				AND a.nombre_pelicula IS NOT NULL
				AND a.nombre_pelicula != '-'
				AND a.nombre_tienda IS NOT NULL
				AND a.nombre_tienda != '-'
				AND a.nombre_cliente IS NOT NULL
				AND a.nombre_cliente != '-'
				AND a.ano_lanzamiento IS NOT NULL
				AND a.ano_lanzamiento != '-'
				GROUP BY a.nombre_pelicula,a.nombre_tienda, a.nombre_cliente,a.ano_lanzamiento, p.id_pelicula, t.id_tienda
				ORDER BY a.nombre_pelicula
			) AS a
		GROUP BY a.nombre_pelicula, a.nombre_tienda, a.id_pelicula, a.id_tienda
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
;"""

eliminarModelo = """DROP TABLE RENTA;
DROP TABLE CLIENTE;
DROP TABLE EMPLEADO;
DROP TABLE INVENTARIO;
DROP TABLE TIENDA;
DROP TABLE DIRECCION;
DROP TABLE TRADUCCION;
DROP TABLE DETALLE_CATEGORIA;
DROP TABLE ELENCO;
DROP TABLE PELICULA;
DROP TABLE CIUDAD;
DROP TABLE ACTOR;
DROP TABLE CLASIFICACION;
DROP TABLE IDIOMA;
DROP TABLE CATEGORIA;
DROP TABLE PAIS;"""


eliminarTemporal = """DELETE FROM datos;"""

crearTemporal = """CREATE TEMPORARY TABLE datos (
	nombre_cliente VARCHAR (100),
	correo_cliente VARCHAR (100),
	cliente_activo VARCHAR (3),
	fecha_creacion VARCHAR (100),
	tienda_preferida VARCHAR (100),
	direccion_cliente VARCHAR (100),
	codigo_postal_cliente VARCHAR (100),
	ciudad_cliente VARCHAR (100),
	pais_cliente VARCHAR (100),
	fecha_renta VARCHAR (100),
	fecha_retorno VARCHAR (100),
	monto_a_pagar VARCHAR (100),
	fecha_pago VARCHAR (100),
	nombre_empleado VARCHAR (100),
	correo_empleado VARCHAR (100),
	empleado_activo VARCHAR (3),
	tienda_empleado VARCHAR (100),
	usuario_empleado VARCHAR (100),
	contrasena_empleado VARCHAR (100),
	direccion_empleado VARCHAR (100),
	codigo_postal_empleado VARCHAR (100),
	ciudad_empleado VARCHAR (100),
	pais_empleado VARCHAR (100),
	nombre_tienda VARCHAR (100),
	encargado_tienda VARCHAR (100),
	direccion_tienda VARCHAR (100),
	codigo_postal_tienda VARCHAR (100),
	ciudad_tienda VARCHAR (100),
	pais_tienda VARCHAR (100),
	tienda_pelicula VARCHAR (100),
	nombre_pelicula VARCHAR (100),
	descripcion_pelicula VARCHAR (200),
	ano_lanzamiento VARCHAR (100),
	dias_renta VARCHAR (100),
	costo_renta VARCHAR (100),
	duracion VARCHAR (100),
	costo_por_dano VARCHAR (100),
	clasificacion VARCHAR (100),
	lenguaje_pelicula VARCHAR (100),
	categoria_pelicula VARCHAR (100),
	actor_pelicula VARCHAR (100)
);"""

consulta1 = """
SELECT p.titulo as pelicula, SUM(i.cantidad)as inventario FROM inventario as i
INNER JOIN PELICULA as p
ON i.fk_id_pelicula = p.id_pelicula
WHERE p.titulo = 'SUGAR WONKA'
GROUP BY i.fk_id_pelicula, p.titulo
;
"""

consulta2 = """
SELECT a.nombre_cliente as nombre, a.apellido_cliente as apellido,a.conteo as rentas, CAST (a.pagado AS FLOAT4)  FROM
	(
		SELECT fk_id_cliente as id_cliente, COUNT(fk_id_cliente) as conteo, 
		c.nombre_cliente,c.apellido_cliente, SUM(cantidad_pagar) as pagado FROM renta as a
		FULL OUTER JOIN CLIENTE as c
		ON a.fk_id_cliente = c.id_cliente
		GROUP BY fk_id_cliente,c.nombre_cliente,c.apellido_cliente
		ORDER BY c.nombre_cliente
	)AS a
	WHERE conteo >= 40
;"""

consulta3 = """
SELECT nombre_actor || ' ' || apellido_actor as nombre_actor from ACTOR 
WHERE apellido_actor LIKE 'Son%'
OR apellido_actor LIKE '%son%'
ORDER BY nombre_actor;
"""

consulta4 = """
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
"""
consulta5 = """
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
	SELECT nombre, no_peliculas, (no_peliculas::FLOAT4*100) /
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
	
		)
	as porcentaje, nombre_pais FROM consulta5	
;
"""

consulta6 = """	
	SELECT COUNT(c.id_ciudad) as clientes_por_ciudad, ROUND(CAST(COUNT(c.id_ciudad) AS NUMERIC)/CAST(e.conteo AS NUMERIC),3)*100::FLOAT4 as porcentaje,  
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
"""

consulta62 = """WITH
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
		SELECT CAST ((a.no_pais/(SELECT COUNT(id_renta) FROM renta))*100 AS FLOAT4) as porcentaje_pais,a.nombre_pais FROM
		(
			SELECT SUM(no_peliculas) as no_pais, nombre_pais, id_pais FROM paises
			GROUP BY nombre_pais, id_pais
			ORDER BY nombre_pais
		) AS a
		ORDER BY a.no_pais DESC;
"""

consulta7 = """
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
		a.no_peliculas as rentas, ROUND(CAST((CAST (a.no_peliculas AS NUMERIC(5,2))/CAST(b.conteo_ciudad AS NUMERIC(5,2))) AS NUMERIC(5,2)),2)::FLOAT4 as promedio
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

"""

consulta8 = """	
	SELECT COUNT(e.nombre_pais) rentas, ROUND(CAST(CAST(COUNT(e.nombre_pais) AS NUMERIC (6,2))/CAST(i.rentas  AS NUMERIC(6,2)) AS NUMERIC(6,2)),3)*100::FLOAT4 as porcentaje,
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
"""

consulta9 = """WITH
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
"""


consulta10 = """
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
"""

try:
    con = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST)
    
    cur = con.cursor()
    
    print(con.status)
    

    @app.route("/")
    def hello():
      return "<h1 style='color:blue'>Inicio</h1>"

#obtengo todos los registros de mi tabla movies que cree en mi BD
    @app.route('/consulta1', methods=['GET'])
    def fetch_consulta1():
        try:
            cur.execute(consulta1)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result =[]
            for row in rows:
                row = dict(zip(columns,row))
                result.append(row)
            
            #row = {"columns":columns, "rows":rows, "result":result}
            #print(rows)
            con.commit()
            return jsonify(result)
        except:
            return jsonify('Error, los datos no existen.')
    @app.route('/consulta2', methods=['GET'])
    def fetch_consulta2():
        cur.execute(consulta2)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result)

    @app.route('/consulta3', methods=['GET'])
    def fetch_consulta3():
        cur.execute(consulta3)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result)

    @app.route('/consulta4', methods=['GET'])
    def fetch_consulta4():
        cur.execute(consulta4)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result) 

    @app.route('/consulta5', methods=['GET'])
    def fetch_consulta5():
        cur.execute(consulta5)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result) 

    @app.route('/consulta6', methods=['GET'])
    def fetch_consulta6():
        cur.execute(consulta6)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result) 


    @app.route('/consulta62', methods=['GET'])
    def fetch_consulta62():
        cur.execute(consulta62)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result) 


    @app.route('/consulta7', methods=['GET'])
    def fetch_consulta7():
        cur.execute(consulta7)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result) 
    
    @app.route('/consulta8', methods=['GET'])
    def fetch_consulta8():
        cur.execute(consulta8)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        con.commit()
        return jsonify(result)

    @app.route('/consulta9', methods=['GET'])
    def fetch_consulta9():
        cur.execute(consulta9)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        con.commit()
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        return jsonify(result)

    @app.route('/consulta10', methods=['GET'])
    def fetch_consulta10():
        cur.execute(consulta10)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result =[]
        for row in rows:
            row = dict(zip(columns,row))
            result.append(row)
        con.commit()
        #row = {"columns":columns, "rows":rows, "result":result}
        #print(rows)
        return jsonify(result)

    @app.route('/cargarTemporal', methods=['GET'])
    def fetch_cargarTemporal():
        cur.execute(crearTemporal)  
        con.commit()
        with open ('/tmp/convertido.csv','r') as f:
            next(f)
            cur.copy_from(f, 'datos',sep=';')
        con.commit()
        return jsonify('Temporal cargada.') 

    @app.route('/eliminarTemporal', methods=['GET'])
    def fetch_eliminarTemporal():
        cur.execute(eliminarTemporal)
        con.commit()
        return jsonify('Temporal eliminada.')

    @app.route('/cargarModelo', methods=['GET'])
    def fetch_cargarModelo():
        cur.execute(crearModelo)
        cur.execute(cargarModelo)
        con.commit()
        return jsonify('Modelo cargado.')        

    @app.route('/eliminarModelo', methods=['GET'])
    def fetch_eliminarModelo():
        cur.execute(eliminarModelo)
        con.commit()
        return jsonify('Modelo eliminado.')
    if __name__ == "__main__":
     app.run(host='0.0.0.0')    

except ValueError as e: 
    print(e)