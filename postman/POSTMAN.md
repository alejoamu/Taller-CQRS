# API Bank Account — Postman y Newman

Esta carpeta contiene la colección HTTP que ejercita los controladores de `account.cmd` (comandos) y `account.query` (consultas), con scripts de prueba compatibles con **Postman** y **Newman**.

## Requisitos previos

1. Infraestructura en marcha según el proyecto: **Kafka**, **MongoDB** (event store / comandos), **MySQL** (lectura), etc.
2. Arrancar **`account.cmd`** (puerto por defecto **5000**) y **`account.query`** (puerto por defecto **5001**).
3. Para **Newman**: Node.js instalado.

## Archivos

| Archivo | Uso |
|--------|-----|
| `Bank-Account-API.postman_collection.json` | Colección: peticiones ordenadas y tests |
| `local.postman_environment.json` | Variables `baseUrlCmd`, `baseUrlQry`, `accountHolder` |

## Variables de entorno (Postman)

Importa `local.postman_environment.json` o crea un entorno manual con:

| Variable | Valor por defecto | Descripción |
|----------|-------------------|-------------|
| `baseUrlCmd` | `http://localhost:5000` | API de comandos (`CommandApplication`) |
| `baseUrlQry` | `http://localhost:5001` | API de consultas (`QueryApplication`) |
| `accountHolder` | `NewmanTestUser` | Titular usado al abrir cuenta (debe coincidir en GET por titular) |
| `accountId` | *(vacío)* | Se rellena automáticamente tras **POST** abrir cuenta |

## Flujo de la colección (orden 1 → 6)

1. **OpenAccountController** — `POST {{baseUrlCmd}}/api/v1/openBankAccount`  
   - Cuerpo JSON: `accountHolder`, `accountType` (`SAVINGS` o `CURRENT`), `openingBalance`.  
   - Respuesta **201**: cuerpo tipo `OpenAccountResponse` con `message` e **`id`** (UUID). El test guarda `id` en la variable de entorno `accountId`.

2. **DepositFundsController** — `PUT {{baseUrlCmd}}/api/v1/depositFunds/{{accountId}}`  
   - Cuerpo: `{ "amount": 50.0 }`.  
   - Respuesta **200**: `BaseResponse` con `message`.

3. **WithdrawFundsController** — `PUT {{baseUrlCmd}}/api/v1/withdrawFunds/{{accountId}}`  
   - Cuerpo: `{ "amount": 25.0 }`.  
   - Respuesta **200**.

4. **CloseAccountController** — `DELETE {{baseUrlCmd}}/api/v1/closeBankAccount/{{accountId}}`  
   - Sin cuerpo. Respuesta **200**.

5. **AccountLookupController** — todos contra `{{baseUrlQry}}`  
   - **a)** `GET /api/v1/bankAccountLookup/` — todas las cuentas (**200** con JSON o **204** si no hay datos).  
   - **b)** `GET /api/v1/bankAccountLookup/byId/{{accountId}}`  
   - **c)** `GET /api/v1/bankAccountLookup/byHolder/{{accountHolder}}`  
   - **d)** `GET /api/v1/bankAccountLookup/withBalance/{equalityType}/{balance}`  
     - `equalityType`: `GREATER_THAN` o `LESS_THAN` (enum del backend).  
     - Ejemplo en colección: `GREATER_THAN` y balance `100` (el flujo de prueba deja saldo 125: 100 + 50 − 25).

6. **RestoreReadDbController** — `POST {{baseUrlCmd}}/api/v1/restoreReadDb`  
   - Sin cuerpo. Respuesta **201** (`CREATED` en el controlador).

### Nota sobre CQRS y el orden 4 → 5

Los comandos se publican por Kafka y la vista de lectura se actualiza de forma **asíncrona**. Si necesitas ver la cuenta en los GET **antes** de cerrarla, en Postman puedes **mover** la carpeta **5 - AccountLookupController** para ejecutarla **antes** de **4 - CloseAccountController**, o duplicar las peticiones GET en ese punto. La colección sigue el orden pedido (cierre y luego consultas); los tests aceptan **200** o **204** en los GET para reducir fallos por timing.

## Cómo usarlo en Postman

1. **Importar** → *File* → `Bank-Account-API.postman_collection.json`.  
2. **Importar** entorno → `local.postman_environment.json`.  
3. Seleccionar el entorno **Bank Account - Local** en el desplegable superior.  
4. Ejecutar **Run collection** (o carpeta por carpeta). La primera petición rellena `accountId` para el resto.

**Guardar el `id` manualmente (si no usas la colección):** en la respuesta de abrir cuenta, copia el campo `id` y pégalo en la variable de colección/entorno `accountId`.

## Newman (CLI)

Desde la raíz del repositorio (ajusta rutas si hace falta):

```bash
npx --yes newman run postman/Bank-Account-API.postman_collection.json -e postman/local.postman_environment.json
```

Opcional: pausa entre peticiones para dar margen al consumidor Kafka (por ejemplo 1 s):

```bash
npx --yes newman run postman/Bank-Account-API.postman_collection.json -e postman/local.postman_environment.json --delay-request 1000
```

Salida HTML o JUnit (CI):

```bash
npx --yes newman run postman/Bank-Account-API.postman_collection.json -e postman/local.postman_environment.json -r cli,htmlextra --reporter-htmlextra-export postman/newman-report.html
```

*(El reporter `htmlextra` requiere `npm i -g newman-reporter-htmlextra` o uso con `npx` según tu instalación.)*

## Referencia rápida de rutas (código)

| Controlador | Método | Ruta (relativa a CMD o Query) |
|-------------|--------|-------------------------------|
| OpenAccount | POST | `/api/v1/openBankAccount` (CMD) |
| DepositFunds | PUT | `/api/v1/depositFunds/{id}` (CMD) |
| WithdrawFunds | PUT | `/api/v1/withdrawFunds/{id}` (CMD) |
| CloseAccount | DELETE | `/api/v1/closeBankAccount/{id}` (CMD) |
| AccountLookup | GET | `/api/v1/bankAccountLookup/...` (Query) |
| RestoreReadDb | POST | `/api/v1/restoreReadDb` (CMD) |

Los puertos vienen de `account.cmd/src/main/resources/application.yml` (5000) y `account.query/src/main/resources/application.yml` (5001).
