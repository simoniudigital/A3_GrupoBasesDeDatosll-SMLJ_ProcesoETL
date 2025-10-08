--Data Mart (Esquema dm) - Drop/Create
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dm')
  EXEC('CREATE SCHEMA dm AUTHORIZATION dbo');
GO

USE JardineriaStaging;

IF OBJECT_ID('dm.FactPagos')   IS NOT NULL DROP TABLE dm.FactPagos;
IF OBJECT_ID('dm.FactVentas')  IS NOT NULL DROP TABLE dm.FactVentas;
IF OBJECT_ID('dm.DimProducto') IS NOT NULL DROP TABLE dm.DimProducto;
IF OBJECT_ID('dm.DimCategoria') IS NOT NULL DROP TABLE dm.DimCategoria;
IF OBJECT_ID('dm.DimCliente')  IS NOT NULL DROP TABLE dm.DimCliente;
IF OBJECT_ID('dm.DimTiempo')   IS NOT NULL DROP TABLE dm.DimTiempo;
GO

CREATE TABLE dm.DimCliente(
  cliente_sk INT IDENTITY(1,1) PRIMARY KEY,
  cliente_id INT UNIQUE,
  nombre_cliente VARCHAR(60),
  ciudad VARCHAR(60),
  pais   VARCHAR(60),
  limite_credito DECIMAL(15,2),
  vigente BIT DEFAULT 1,
  fecha_ini DATE DEFAULT CAST(GETDATE() AS DATE),
  fecha_fin DATE NULL
);

CREATE TABLE dm.DimCategoria(
  categoria_sk INT IDENTITY(1,1) PRIMARY KEY,
  categoria_id INT UNIQUE,
  desc_categoria VARCHAR(60)
);

CREATE TABLE dm.DimTiempo(
  tiempo_sk INT IDENTITY(1,1) PRIMARY KEY,
  Fecha DATE UNIQUE,
  Anio INT, Mes INT, Dia INT, Trimestre INT,
  NombreMes VARCHAR(15), NombreDia VARCHAR(15)
);

CREATE TABLE dm.DimProducto(
  producto_sk INT IDENTITY(1,1) PRIMARY KEY,
  producto_id INT UNIQUE,
  codigo_producto VARCHAR(30),
  nombre_producto VARCHAR(100),
  proveedor VARCHAR(100),
  precio_venta DECIMAL(15,2),
  categoria_sk INT NOT NULL,
  CONSTRAINT FK_DimProducto_DimCategoria
    FOREIGN KEY (categoria_sk) REFERENCES dm.DimCategoria(categoria_sk)
);

CREATE TABLE dm.FactVentas(
  venta_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
  pedido_id INT,
  detalle_pedido_id INT,
  producto_sk INT NOT NULL,
  cliente_sk  INT NOT NULL,
  tiempo_sk   INT NOT NULL,
  cantidad INT NOT NULL,
  precio_unitario DECIMAL(15,2) NOT NULL,
  total_linea AS (cantidad * precio_unitario) PERSISTED,
  CONSTRAINT FK_FV_Prod FOREIGN KEY (producto_sk) REFERENCES dm.DimProducto(producto_sk),
  CONSTRAINT FK_FV_Clie FOREIGN KEY (cliente_sk)  REFERENCES dm.DimCliente(cliente_sk),
  CONSTRAINT FK_FV_Time FOREIGN KEY (tiempo_sk)   REFERENCES dm.DimTiempo(tiempo_sk)
);

CREATE TABLE dm.FactPagos(
  pago_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
  pago_id INT,
  cliente_sk INT NOT NULL,
  tiempo_sk  INT NOT NULL,
  total DECIMAL(15,2) NOT NULL,
  CONSTRAINT FK_FP_Clie FOREIGN KEY (cliente_sk) REFERENCES dm.DimCliente(cliente_sk),
  CONSTRAINT FK_FP_Time FOREIGN KEY (tiempo_sk)  REFERENCES dm.DimTiempo(tiempo_sk)
);

--Índices Útiles
CREATE INDEX IX_FV_consulta ON dm.FactVentas(cliente_sk, producto_sk, tiempo_sk);
CREATE INDEX IX_FP_consulta ON dm.FactPagos(cliente_sk, tiempo_sk);
GO

--Carga Dimensiones desde Staging
BEGIN TRAN;

--DimTiempo
INSERT INTO dm.DimTiempo(Fecha, Anio, Mes, Dia, Trimestre, NombreMes, NombreDia)
SELECT s.Fecha, s.Anio, s.Mes, s.Dia, s.Trimestre, s.NombreMes, s.NombreDia
FROM dbo.DimTiempo s
LEFT JOIN dm.DimTiempo d ON d.Fecha = s.Fecha
WHERE d.Fecha IS NULL;

--DimCliente
MERGE dm.DimCliente AS tgt
USING (
  SELECT ID_Cliente AS cliente_id, Nombre_Cliente, Ciudad, Pais, Limite_Credito
  FROM dbo.DimCliente
) AS src
ON (tgt.cliente_id = src.cliente_id AND tgt.vigente = 1)
WHEN MATCHED AND (
     ISNULL(tgt.nombre_cliente,'') <> ISNULL(src.Nombre_Cliente,'')
  OR ISNULL(tgt.ciudad,'')         <> ISNULL(src.Ciudad,'')
  OR ISNULL(tgt.pais,'')           <> ISNULL(src.Pais,'')
  OR ISNULL(tgt.limite_credito,0)  <> ISNULL(src.Limite_Credito,0)
) THEN UPDATE SET
     nombre_cliente = src.Nombre_Cliente,
     ciudad         = src.Ciudad,
     pais           = src.Pais,
     limite_credito = src.Limite_Credito
WHEN NOT MATCHED THEN
  INSERT (cliente_id, nombre_cliente, ciudad, pais, limite_credito)
  VALUES (src.cliente_id, src.Nombre_Cliente, src.Ciudad, src.Pais, src.Limite_Credito);

--DimCategoria
MERGE dm.DimCategoria AS tgt
USING (
  SELECT Id_Categoria AS categoria_id, Desc_Categoria
  FROM dbo.DimCategoria
) AS src
ON (tgt.categoria_id = src.categoria_id)
WHEN MATCHED AND ISNULL(tgt.desc_categoria,'') <> ISNULL(src.Desc_Categoria,'')
THEN UPDATE SET desc_categoria = src.Desc_Categoria
WHEN NOT MATCHED THEN
  INSERT (categoria_id, desc_categoria)
  VALUES (src.categoria_id, src.Desc_Categoria);

--DimProducto
;WITH cat AS (
  SELECT categoria_id, categoria_sk FROM dm.DimCategoria
)
MERGE dm.DimProducto AS tgt
USING (
  SELECT p.ID_Producto AS producto_id, p.CodigoProducto, p.Nombre, p.Proveedor, p.Precio_Venta, p.Categoria
  FROM dbo.DimProducto p
) AS src
ON (tgt.producto_id = src.producto_id)
WHEN MATCHED AND (
     ISNULL(tgt.codigo_producto,'') <> ISNULL(src.CodigoProducto,'')
  OR ISNULL(tgt.nombre_producto,'') <> ISNULL(src.Nombre,'')
  OR ISNULL(tgt.proveedor,'')       <> ISNULL(src.Proveedor,'')
  OR ISNULL(tgt.precio_venta,0)     <> ISNULL(src.Precio_Venta,0)
  OR tgt.categoria_sk               <> (SELECT categoria_sk FROM cat WHERE cat.categoria_id = src.Categoria)
) THEN UPDATE SET
     codigo_producto = src.CodigoProducto,
     nombre_producto = src.Nombre,
     proveedor       = src.Proveedor,
     precio_venta    = src.Precio_Venta,
     categoria_sk    = (SELECT categoria_sk FROM cat WHERE cat.categoria_id = src.Categoria)
WHEN NOT MATCHED THEN
  INSERT (producto_id, codigo_producto, nombre_producto, proveedor, precio_venta, categoria_sk)
  VALUES (
    src.producto_id, src.CodigoProducto, src.Nombre, src.Proveedor, src.Precio_Venta,
    (SELECT categoria_sk FROM cat WHERE cat.categoria_id = src.Categoria)
  );

COMMIT;
GO

--Carga hechos desde staging
BEGIN TRAN;

--FactVentas
;WITH map_cl AS (
  SELECT cliente_id, cliente_sk FROM dm.DimCliente WHERE vigente = 1
),
map_pr AS (
  SELECT producto_id, producto_sk FROM dm.DimProducto
),
map_tm AS (
  SELECT Fecha, tiempo_sk FROM dm.DimTiempo
),
src AS (
  SELECT dp.ID_Detalle_Pedido,
         dp.ID_Pedido,
         dp.ID_Producto,
         dp.Cantidad,
         dp.Precio_Unidad,
         p.ID_Cliente,
         p.Fecha_Pedido
  FROM dbo.FactDetallePedido dp
  INNER JOIN dbo.FactPedido p ON p.ID_Pedido = dp.ID_Pedido
)
INSERT INTO dm.FactVentas(pedido_id, detalle_pedido_id, producto_sk, cliente_sk, tiempo_sk, cantidad, precio_unitario)
SELECT s.ID_Pedido,
       s.ID_Detalle_Pedido,
       mp.producto_sk,
       mc.cliente_sk,
       mt.tiempo_sk,
       s.Cantidad,
       s.Precio_Unidad
FROM src s
INNER JOIN map_pr mp ON mp.producto_id = s.ID_Producto
INNER JOIN map_cl mc ON mc.cliente_id  = s.ID_Cliente
INNER JOIN map_tm mt ON mt.Fecha       = s.Fecha_Pedido;

--FactPagos
;WITH map_cl AS (
  SELECT cliente_id, cliente_sk FROM dm.DimCliente WHERE vigente = 1
),
map_tm AS (
  SELECT Fecha, tiempo_sk FROM dm.DimTiempo
)
INSERT INTO dm.FactPagos(pago_id, cliente_sk, tiempo_sk, total)
SELECT pg.ID_Pago,
       mc.cliente_sk,
       mt.tiempo_sk,
       pg.Total
FROM dbo.FactPago pg
INNER JOIN map_cl mc ON mc.cliente_id = pg.ID_Cliente
INNER JOIN map_tm mt ON mt.Fecha      = pg.Fecha_Pago;

COMMIT;
GO

--Validaciones

--Conteos
SELECT 'DimCliente' AS Tabla, COUNT(*) AS Registros FROM dm.DimCliente
UNION ALL SELECT 'DimCategoria', COUNT(*) FROM dm.DimCategoria
UNION ALL SELECT 'DimProducto', COUNT(*) FROM dm.DimProducto
UNION ALL SELECT 'DimTiempo', COUNT(*) FROM dm.DimTiempo
UNION ALL SELECT 'FactVentas', COUNT(*) FROM dm.FactVentas
UNION ALL SELECT 'FactPagos', COUNT(*) FROM dm.FactPagos;

--Nulos en SKs
SELECT TOP 50 * FROM dm.FactVentas WHERE producto_sk IS NULL OR cliente_sk IS NULL OR tiempo_sk IS NULL;
SELECT TOP 50 * FROM dm.FactPagos  WHERE cliente_sk IS NULL OR tiempo_sk IS NULL;

--Paridad de totales (Detalle vs DM)
SELECT SUM(CONVERT(DECIMAL(18,2), dp.Cantidad) * dp.Precio_Unidad) AS total_staging
FROM dbo.FactDetallePedido dp;

SELECT SUM(total_linea) AS total_dm
FROM dm.FactVentas;

--Spot check por pedido
SELECT p.ID_Pedido, p.Total AS total_staging,
       SUM(v.total_linea) AS total_dm
FROM dbo.FactPedido p
JOIN dm.FactVentas v ON v.pedido_id = p.ID_Pedido
GROUP BY p.ID_Pedido, p.Total
HAVING SUM(v.total_linea) <> p.Total;

--Totales de pagos por cliente
SELECT pg.ID_Cliente, SUM(pg.Total) AS total_pagado_staging
FROM dbo.FactPago pg
GROUP BY pg.ID_Cliente;

SELECT c.cliente_id, SUM(f.total) AS total_pagado_dm
FROM dm.FactPagos f
JOIN dm.DimCliente c ON c.cliente_sk = f.cliente_sk
GROUP BY c.cliente_id;