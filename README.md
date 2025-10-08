# A3_GrupoBasesDeDatosll-SMLJ_ProcesoETL


**Carrera**

Ingeniería en Software y Datos

**Asignatura**

- Bases de Datos II

**Institución**

- Institución Universitaria Digital de Antioquia

**Docente**

- Antonio Jesús Valderrama Jaramillo

**Integrantes del Equipo**

- Luisa Fernanda Gómez Quiroz,
- Simón Arbey Castaño Ríos,
- Juan Pablo Gonzalez Gil

Este proyecto documenta el proceso completo de diseño e implementación de un Data Mart utilizando el **Modelo Estrella (Star Schema)**, partiendo de la base de datos transaccional (OLTP) de Jardinería. El objetivo es transformar los datos operacionales en información organizada y optimizada para el análisis de ventas y pagos, utilizando una base de datos de Staging como paso intermedio.

## 1. Estructura del Repositorio

| Archivo | Descripción | Fase del Proyecto |
| --- | --- | --- |
| `EA1...ModeloEstrella_Jardineria.pdf` | **Documento de Diseño Dimensional.** Describe el objetivo, las tablas relevantes, y la propuesta final del Modelo Estrella (Data Mart). | Modelo Estrella |
| `EA2...Staging_Jardineria.pdf` | **Documento de Base de Datos Staging.** Detalla el proceso y las estructuras utilizadas para la creación y migración inicial a la base de datos de Staging. | Staging |
| `EA3...ProcesoETL.pdf` | **Documento del Proceso ETL.** Detalla las etapas de Extracción, Transformación y Carga (ETL) para poblar el Data Mart desde la base de datos de Staging, incluyendo revisiones y validaciones. | Proceso ETL |
| `StagingJardineria.sql` | **Script SQL de Creación de Staging.** Contiene las instrucciones para crear la base de datos intermedia (`JardineriaStaging`) y migrar los datos brutos desde la base de datos original de `jardineria`. | Staging |
| `DataMart.sql` | **Script SQL de Creación y Carga del Data Mart.** Define la estructura final del Data Mart (esquema `dm`) y ejecuta las transformaciones para poblar las tablas de Dimensiones y Hechos. | Data Mart y ETL |
| `jardineria.bak` / `staging_jardineria.bak` | Archivos de respaldo de la base de datos original y la base de datos de Staging. | Respaldos |

Exportar a Hojas de cálculo

---

## 2. Arquitectura del Data Mart (Modelo Estrella)

El Data Mart está diseñado para el análisis de ventas y pagos de la empresa Jardinería, utilizando un esquema de estrella.

### Tablas de Hechos (Fact Tables)

Contienen las métricas y claves foráneas a las dimensiones.

- **`dm.FactVentas`**: Registra cada línea de detalle de pedido, incluyendo la cantidad vendida, el precio unitario, y el total por línea. Su granularidad es a nivel de detalle de pedido.
- **`dm.FactPagos`**: Resume los pagos realizados por los clientes.

### Tablas de Dimensiones (Dimension Tables)

Contienen los atributos descriptivos para el análisis.

- **`dm.DimCliente`**: Información del cliente (nombre, ciudad, país, límite de crédito). Incluye atributos para manejar cambios lentos (claves sustitutas, `vigente`, `fecha_ini`, `fecha_fin`).
- **`dm.DimProducto`**: Detalles del producto (código, nombre, proveedor, precio de venta).
- **`dm.DimCategoria`**: Descripción de las categorías de los productos.
- **`dm.DimTiempo`**: Generada para facilitar el análisis temporal por año, mes, día y trimestre, a partir de las fechas de pedidos y pagos.

---

## 3. Proceso de Implementación (ETL)

La implementación se basa en tres pasos principales, asumiendo que el motor de base de datos es **SQL Server**.

### Prerrequisitos

1. Tener una instancia de **SQL Server** disponible.
2. La base de datos original (`jardineria`, OLTP) debe estar restaurada o creada y poblada, ya que los scripts de Staging extraen datos de ella.

### Pasos de Ejecución

### Paso 1: Creación y Carga de la Base de Datos Staging

La base de datos de Staging es una copia de trabajo que se utiliza para realizar la limpieza y la transformación de datos antes de cargarlos en el Data Mart.

1. Ejecute el script **`StagingJardineria.sql`**.
2. Este script crea la base de datos **`JardineriaStaging`** y sus tablas intermedias (con prefijo `Dim` y `Fact`, como `DimCliente`, `FactPedido`, etc.).
3. El script realiza la **Extracción (E)** y **Carga (L)** de los datos desde la base de datos `jardineria` original hacia las tablas de Staging.

### Paso 2: Proceso ETL Completo y Construcción del Data Mart

Este paso aplica las transformaciones y la carga final al Data Mart.

1. Asegúrese de que el script **`StagingJardineria.sql`** haya sido ejecutado y corregido (el documento `EA3...ProcesoETL.pdf` menciona una inconsistencia en el nombre de la base de datos, usando `JardineriaStaging` consistentemente es la recomendación).
2. Ejecute el script **`DataMart.sql`**.
3. El script realiza las siguientes acciones:
    - Crea el esquema `dm`.
    - Crea las tablas de dimensiones y hechos finales (`dm.DimCliente`, `dm.FactVentas`, etc.).
    - Ejecuta el proceso de **Transformación (T)** y **Carga (L)**: Inserta los datos desde las tablas de Staging (`JardineriaStaging`) a las tablas del Data Mart (`dm`), generando las **claves sustitutas (`_sk`)** y aplicando transformaciones como la generación de la dimensión de tiempo y la corrección de datos.
4. El script incluye validaciones al final para verificar los conteos de registros y la paridad de los totales.
