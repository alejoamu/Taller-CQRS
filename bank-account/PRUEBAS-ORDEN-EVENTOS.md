# Pruebas: orden de consumo de eventos (un solo topic)

## Cambios aplicados (resumen)

- Todos los eventos se publican al topic **`BankAccountEvents`** (`spring.kafka.topic`).
- La clave del mensaje Kafka es el **id del agregado** (`event.getId()`), de modo que todos los eventos de una misma cuenta van a la **misma partición** y Kafka conserva su **orden relativo** dentro de esa partición.
- El consumidor usa **un único** `@KafkaListener` sobre ese topic y despacha con reflexión al `EventHandler.on(...)`.

## Infraestructura

1. Kafka, MongoDB y MySQL según tu entorno (en `application.yml` el bootstrap de Kafka es `localhost:29092`; ajústalo si usas otro puerto).
2. Crea el topic si no existe, por ejemplo **1 partición** para laboratorios sencillos (orden global trivial), o varias particiones sabiendo que el orden solo se garantiza **por partición** (de ahí el uso de clave = `aggregateId`).

Ejemplo (CLI Kafka):

```bash
kafka-topics.sh --create --topic BankAccountEvents --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1
```

## Evidencia en logs (capturas de pantalla)

1. Arranca **`account.cmd`** y **`account.query`** con nivel **INFO** (por defecto en Spring Boot).
2. Ejecuta en secuencia (Postman, Newman o `curl`):
   - Abrir cuenta → Depositar → Retirar → Cerrar cuenta.
3. En la consola de **`account.query`** busca líneas como:

   `[Kafka] Consumed from BankAccountEvents | type=... aggregateId=... aggregateVersion=...`

4. **Captura 1:** bloque de logs donde se vea la secuencia **AccountOpenedEvent → FundsDepositedEvent → FundsWithdrawnEvent → AccountClosedEvent** para el **mismo** `aggregateId`, con `aggregateVersion` creciente (1, 2, 3, 4…).
5. **Reproducción tras reinicio:**
   - Opción A: Detén solo **`account.query`**, bórralo o vacía la tabla de cuentas en MySQL si quieres ver la reconstrucción desde cero, vuelve a arrancar **`account.query`** y deja que el consumer lea desde `auto-offset-reset: earliest` (o desde el offset del grupo `bankaccConsumer`).
   - Opción B: Usa **POST** `/api/v1/restoreReadDb` en el servicio de comandos (si ya lo tienes integrado) para volver a publicar eventos desde el event store y observa de nuevo el orden en los logs del query.
6. **Captura 2:** logs tras el reinicio (o tras restore) mostrando otra vez el mismo orden de tipos/versiones para esa cuenta.

## Por qué importa el orden (CQRS + Event Sourcing)

En **Event Sourcing**, el estado del agregado (y de la proyección de lectura) es la **aplicación secuencial** de eventos. Si un consumidor aplicara **“Depositar” antes que “Cuenta abierta”**, el saldo o la fila en MySQL serían **incorrectos**. Por eso necesitas:

- **Una cola ordenada por agregado** (misma partición vía misma **message key**, o un solo topic con una partición en demos), y
- **Un solo consumer por partición** procesando en orden de offset.

Kafka garantiza orden **por partición**, no entre particiones distintas.
