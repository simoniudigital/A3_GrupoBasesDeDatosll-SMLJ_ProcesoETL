DROP DATABASE IF EXISTS JardineriaStaging;
GO

CREATE DATABASE StagingJardineria;
GO

USE JardineriaStaging;
GO

--Tablas Dimensiones--
CREATE TABLE DIMOficina(
ID_Oficina INT PRIMARY KEY,
Ciudad VARCHAR (100),
Pais VARCHAR (100),
Region VARCHAR (100),
);

CREATE TABLE DimEmpleado(
ID_Empleado INT PRIMARY KEY,
Nombre VARCHAR (60),
Apellido1 VARCHAR (60),
Apellido2 VARCHAR (60),
Puesto VARCHAR (60),
ID_Oficina INT
);

CREATE TABLE DimCliente(
  ID_cliente INT PRIMARY KEY,
  Nombre_Cliente VARCHAR(60),
  Ciudad VARCHAR(60),
  Pais VARCHAR(60),
  Limite_Credito NUMERIC(15,2)
);

CREATE TABLE DimCategoria(
  Id_Categoria INT PRIMARY KEY,
  Desc_Categoria VARCHAR(60)
);

CREATE TABLE DimProducto(
  ID_producto INT PRIMARY KEY,
  CodigoProducto VARCHAR(30),
  Nombre VARCHAR(100),
  Categoria INT,
  Proveedor VARCHAR(100),
  Precio_Venta NUMERIC(15,2)
);

--Tablas Hechos--
CREATE TABLE FactPedido(
  ID_pedido INT PRIMARY KEY,
  Fecha_Pedido DATE,
  Estado VARCHAR(15),
  ID_Cliente INT,
  Total DECIMAL(15,2)
);

CREATE TABLE FactDetallePedido(
  ID_Detalle_Pedido INT PRIMARY KEY,
  ID_Pedido INT,
  ID_Producto INT,
  Cantidad INT,
  Precio_Unidad DECIMAL(15,2)
);

CREATE TABLE FactPago(
  ID_Pago INT PRIMARY KEY,
  ID_Cliente INT,
  Fecha_Pago DATE,
  Total DECIMAL(15,2)
);

--Migración de datos desde jardineria--

--Dimensiones--
INSERT INTO DIMOficina
SELECT ID_Oficina, Ciudad, Pais, Region
FROM jardineria.dbo.oficina;

INSERT INTO DimEmpleado
SELECT ID_Empleado, Nombre, Apellido1, Apellido2, Puesto, ID_Oficina
FROM jardineria.dbo.empleado;

INSERT INTO DimCliente
SELECT ID_Cliente, Nombre_Cliente, Ciudad, Pais, Limite_Credito
FROM jardineria.dbo.cliente;

INSERT INTO DimCategoria
SELECT Id_Categoria, Desc_Categoria
FROM jardineria.dbo.Categoria_producto;

INSERT INTO DimProducto
SELECT ID_Producto, CodigoProducto, Nombre, Categoria, Proveedor, Precio_Venta
FROM jardineria.dbo.producto;

--Hechos--
INSERT INTO FactPedido
SELECT p.ID_Pedido, p.Fecha_Pedido, p.Estado, p.ID_Cliente,
       SUM(dp.Cantidad * dp.Precio_Unidad) AS Total
FROM jardineria.dbo.pedido p
LEFT JOIN jardineria.dbo.Detalle_Pedido dp ON p.ID_Pedido = dp.ID_Pedido
GROUP BY p.ID_Pedido, p.Fecha_Pedido, p.Estado, p.ID_Cliente;

INSERT INTO FactDetallePedido
SELECT ID_Detalle_Pedido, ID_Pedido, ID_Producto, Cantidad, Precio_Unidad
FROM jardineria.dbo.detalle_pedido

INSERT INTO JardineriaStaging.dbo.FactPago (ID_pago, ID_cliente, fecha_pago, total)
SELECT ID_pago, ID_cliente, fecha_pago, total
FROM jardineria.dbo.pago;

--Validar la carga--

--Cliente--
SELECT COUNT(*) AS Original FROM jardineria.dbo.cliente;
SELECT COUNT(*) AS Staging FROM JardineriaStaging.dbo.DimCliente;

--Pedidos--
SELECT COUNT(*) AS Original FROM jardineria.dbo.pedido;
SELECT COUNT(*) AS Staging FROM JardineriaStaging.dbo.FactPedido;

--Productos--
SELECT COUNT(*) AS Original FROM jardineria.dbo.producto;
SELECT COUNT(*) AS Staging FROM JardineriaStaging.dbo.DimProducto;

--Pagos--
SELECT COUNT(*) AS Original FROM jardineria.dbo.pago;
SELECT COUNT(*) AS Staging FROM JardineriaStaging.dbo.FactPago;

--Backup de Ambas--
BACKUP DATABASE jardineria
TO DISK = 'C:\Backups\jardineria.bak'
WITH FORMAT, INIT, NAME = 'Backup Jardineria';

BACKUP DATABASE StagingJardineria
TO DISK = 'C:\Backups\staging_jardineria.bak'
WITH FORMAT, INIT, NAME = 'Backup Staging Jardineria';


--Evidencia 3 / Data Mart Final--
BEGIN TRAN;

--Eliminar duplicados por PK natural
;WITH d AS (
	SELECT ID_Cliente, ROW_NUMBER() OVER (PARTITION BY ID_Cliente ORDER BY ID_Cliente) rn
	FROM dbo.DimCliente
	)
	DELETE FROM d WHERE rn > 1;

;WITH d AS (
  SELECT ID_Producto, ROW_NUMBER() OVER (PARTITION BY ID_Producto ORDER BY ID_Producto) rn
  FROM dbo.DimProducto
)
DELETE FROM d WHERE rn > 1;

;WITH d AS (
  SELECT ID_Pedido, ROW_NUMBER() OVER (PARTITION BY ID_Pedido ORDER BY ID_Pedido) rn
  FROM dbo.FactPedido
)
DELETE FROM d WHERE rn > 1;

;WITH d AS (
  SELECT ID_Detalle_Pedido, ROW_NUMBER() OVER (PARTITION BY ID_Detalle_Pedido ORDER BY ID_Detalle_Pedido) rn
  FROM dbo.FactDetallePedido
)
DELETE FROM d WHERE rn > 1;

--Estandarizar textos
UPDATE dbo.DimCliente
SET Nombre_Cliente = LTRIM(RTRIM(Nombre_Cliente)),
    Ciudad         = LTRIM(RTRIM(Ciudad)),
    Pais           = LTRIM(RTRIM(Pais));

UPDATE dbo.DimProducto
SET CodigoProducto = UPPER(LTRIM(RTRIM(CodigoProducto))),
    Nombre         = LTRIM(RTRIM(Nombre)),
    Proveedor      = LTRIM(RTRIM(Proveedor));

--Índices de apoyo (Solo si no existen)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DimCliente_ID' AND object_id=OBJECT_ID('dbo.DimCliente'))
  CREATE INDEX IX_DimCliente_ID ON dbo.DimCliente(ID_Cliente);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DimProducto_ID' AND object_id=OBJECT_ID('dbo.DimProducto'))
  CREATE INDEX IX_DimProducto_ID ON dbo.DimProducto(ID_Producto);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DimProducto_Cat' AND object_id=OBJECT_ID('dbo.DimProducto'))
  CREATE INDEX IX_DimProducto_Cat ON dbo.DimProducto(Categoria);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FactPedido' AND object_id=OBJECT_ID('dbo.FactPedido'))
  CREATE INDEX IX_FactPedido ON dbo.FactPedido(ID_Pedido, ID_Cliente);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FactDetPedido_Ped' AND object_id=OBJECT_ID('dbo.FactDetallePedido'))
  CREATE INDEX IX_FactDetPedido_Ped ON dbo.FactDetallePedido(ID_Pedido);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FactDetPedido_Prod' AND object_id=OBJECT_ID('dbo.FactDetallePedido'))
  CREATE INDEX IX_FactDetPedido_Prod ON dbo.FactDetallePedido(ID_Producto);

--Recalcular el total del pedido desde detalle
;WITH tot AS (
  SELECT dp.ID_Pedido, SUM(CONVERT(DECIMAL(15,2), dp.Cantidad) * dp.Precio_Unidad) AS total_calc
  FROM dbo.FactDetallePedido dp
  GROUP BY dp.ID_Pedido
)
UPDATE p
SET p.Total = t.total_calc
FROM dbo.FactPedido p
JOIN tot t ON t.ID_Pedido = p.ID_Pedido;

--DimTiempo Derivada
IF OBJECT_ID('dbo.DimTiempo') IS NOT NULL DROP TABLE dbo.DimTiempo;

CREATE TABLE dbo.DimTiempo(
  tiempo_sk INT IDENTITY(1,1) PRIMARY KEY,
  Fecha DATE UNIQUE,
  Anio  INT,
  Mes   INT,
  Dia   INT,
  Trimestre INT,
  NombreMes  VARCHAR(15),
  NombreDia  VARCHAR(15)
);

;WITH fechas AS (
  SELECT Fecha_Pedido AS Fecha FROM dbo.FactPedido
  UNION
  SELECT Fecha_Pago   AS Fecha FROM dbo.FactPago
)
INSERT INTO dbo.DimTiempo(Fecha, Anio, Mes, Dia, Trimestre, NombreMes, NombreDia)
SELECT f.Fecha,
       YEAR(f.Fecha),
       MONTH(f.Fecha),
       DAY(f.Fecha),
       DATEPART(QUARTER, f.Fecha),
       DATENAME(MONTH, f.Fecha),
       DATENAME(WEEKDAY, f.Fecha)
FROM fechas f
WHERE f.Fecha IS NOT NULL
GROUP BY f.Fecha;

COMMIT;
GO