# Decisiones de diseño

- *Durabilidad de la Work Queue*: la cola es durable y los mensajes persistentes, permitiendo recuperación ante reinicios.
- *Exchange*: se utiliza un exchange directo y durable.
- *Cola del Exchange*: se optó por un modelo de autoborrado, con nombres asignados automáticamente.
- *Prefetch*: se utiliza `prefetch = 1` en la Work Queue para lograr fairness en la distribución entre workers.
- *Ack y Nack*: se implementaron `acks` y `nacks` sin reintento, con posibilidad de requeue. 
- *Publisher Confirms*: se omitió el uso de `NotifyPublish` por su costo de latencia.