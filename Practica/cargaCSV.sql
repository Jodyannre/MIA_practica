--Direccion del SQL
--\i '/home/joddie/Desktop/practica/Practica/cargaCSV.sql'

--Crear la tabla temporal para cargar el csv



CREATE TEMPORARY TABLE datos (
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
);


COPY datos(nombre_cliente,correo_cliente,cliente_activo,fecha_creacion,
tienda_preferida,direccion_cliente,codigo_postal_cliente,ciudad_cliente,
pais_cliente,fecha_renta,fecha_retorno,monto_a_pagar,fecha_pago,
nombre_empleado,correo_empleado,empleado_activo,tienda_empleado,
usuario_empleado,contrasena_empleado,direccion_empleado,
codigo_postal_empleado,ciudad_empleado,pais_empleado,nombre_tienda,
encargado_tienda,direccion_tienda,codigo_postal_tienda,ciudad_tienda,
pais_tienda,tienda_pelicula,nombre_pelicula,descripcion_pelicula,
ano_lanzamiento,dias_renta,costo_renta,duracion,costo_por_dano,
clasificacion,lenguaje_pelicula,categoria_pelicula,actor_pelicula)
FROM '/tmp/BlockbusterData.csv'
DELIMITER ';'
CSV HEADER
encoding 'windows-1251';