## 

- BUDGET: ¿Cuál es el presupuesto máximo para una nueva arquitectura?
- **SLA de 15 minutos:** ¿Cuál es la regla de negocio que dicta este tiempo? ¿Es para monitoreo de inventario en tiempo real o para detección de fraudes?
- **Limitación de 18 horas:** El proceso actual en SQL Server que tarda 18 horas, ¿es por una limitación de hardware/software o es una ventana operativa para no afectar el rendimiento de las tiendas durante el día?
- **Conectividad del ERP:** ¿Qué protocolo permite el ERP para extraer datos? (API, réplica de DB, archivos planos en S3/SFTP).
- **CDC en SQL Server:** ¿La base de datos actual permite la implementación de **Change Data Capture (CDC)** o debemos depender de extracciones por lotes (Batch)?
- **Volumen de Ingesta:** ¿Cuál es el volumen promedio de registros por ventana de 15 minutos y cuál es el pico esperado durante eventos como el "Hot Sale" o Navidad?

---

- **Jerarquía de Verdad:** En caso de discrepancia entre el POS y el ERP, ¿cuál sistema se considera la fuente oficial de la verdad contable?
- **Identificadores Únicos:** ¿Existe un Transaction_ID global que sea consistente entre el E-commerce, el POS y el ERP, o es necesario realizar un mapeo de llaves en la capa Silver?
- **Manejo de Devoluciones:** Dado que el 40% de las ventas son devoluciones, ¿cómo se vincula la devolución con la venta original? ¿Existe una bandera de estado o se genera un nuevo registro compensatorio?
- **Catálogo Maestro:** ¿Existe un MDM para los SKUs o cada sistema maneja sus propias descripciones y categorías?

---

- **Conectividad Inestable:** ¿Cuál es el plan de acción actual cuando una tienda física pierde conexión? ¿Los archivos se acumulan localmente y se envían en ráfaga?
- **Manejo de Datos Tardíos:** ¿Cómo debe reaccionar el pipeline ante registros que llegan con 24 o 48 horas de retraso? ¿Se deben re-procesar los agregados de la capa Gold?
- **Deduplicación:** ¿El sistema fuente garantiza la entrega única o debemos implementar lógica de **Idempotencia** estricta para manejar N cantidad de archivos duplicados?

---

- **Datos Sensibles (PII/PCI):** ¿Los datos de tarjetas o correos electrónicos ya vienen cifrados desde el origen o es responsabilidad del Data Lake aplicar hashing/masking?
- **Retención de Datos:** ¿Existe alguna restricción técnica o legal que obligue a mantener el histórico on-premise en SQL Server por un periodo determinado?
- **Auditoría:** ¿Con qué frecuencia se realizan las conciliaciones financieras y quiénes son los consumidores finales de estos reportes (Finanzas, Operaciones, Auditoría Externa)?

---

- **Proyección de Crecimiento:** ¿Se tiene una estimación de apertura de nuevas tiendas físicas en los próximos 12 a 24 meses?
- **E-commerce:** ¿Es un desarrollo propio o un SaaS (Shopify, VTEX)? ¿Cómo recibe actualmente las actualizaciones de stock desde el almacén central?