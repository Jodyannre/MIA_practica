
--------------------------------------------------------Crear base de datos
CREATE DATABASE practica;

--Conectar a BD
\c practica;

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
	fk_id_tienda INT NOT NULL
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
);


--------------------------------------------------------Borrando base de datos

DROP TABLE RENTA;
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
DROP TABLE PAIS;