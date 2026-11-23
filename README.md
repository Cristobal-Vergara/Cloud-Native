# AndesStay

Sistema de gestión de reservas para hostales y cabañas. Permite administrar alojamientos, reservas, disponibilidad, usuarios y notificaciones.

## Tecnologías

- **Frontend:** React
- **Backend:** Java 17 y Spring Boot
- **BFF:** API intermedia entre el frontend y el backend
- **Base de datos:** MySQL
- **Autenticación:** Microsoft Entra ID
- **Mensajería:** RabbitMQ y Apache Kafka
- **Contenedores:** Docker Compose

## 1. Ejecutar el proyecto en Windows

### Requisitos

- Windows 10 u 11.
- Docker Desktop con contenedores Linux.
- Conexión a Internet para descargar las imágenes y dependencias.
- Una configuración válida de Microsoft Entra ID para utilizar el inicio de sesión.

Desde la carpeta principal del proyecto, ejecuta:

```powershell
.\scripts\start-local.cmd
```

El script prepara la configuración local, construye las aplicaciones e inicia los servicios.

Cuando termine, abre:

http://localhost:5173

Para detener el entorno conservando los datos:

```powershell
.\scripts\stop-local.cmd
```

Las credenciales locales se generan y guardan en `.env`. No compartas ese archivo ni publiques sus contraseñas.

## 2. ¿Cómo funciona la mensajería?

AndesStay utiliza dos tecnologías de mensajería porque cumplen funciones diferentes:

| Tecnología | Función principal |
|---|---|
| RabbitMQ | Distribuir notificaciones y tareas operativas. |
| Apache Kafka | Registrar y procesar eventos del sistema. |

### RabbitMQ: notificaciones y comunicación entre servicios

RabbitMQ se utiliza para distribuir notificaciones relacionadas con las reservas.

Por ejemplo, cuando se confirma una reserva:

1. El backend genera el mensaje.
2. El mensaje se publica en un exchange.
3. Las reglas de routing y bindings determinan a qué colas llega.
4. Los consumidores reciben el mensaje.
5. Se procesa la notificación correspondiente a recepción o al huésped.

**Componentes principales:**

- Exchanges: reciben los mensajes.
- Routing keys: permiten dirigirlos.
- Queues: almacenan los mensajes pendientes.
- Consumers: procesan los mensajes.
- ACK: confirma que un mensaje fue procesado correctamente.
- DLQ: almacena mensajes que no pudieron procesarse después de los reintentos.

El proyecto utiliza confirmación manual de mensajes. Los errores temporales pueden reintentarse y los errores permanentes o agotados se derivan a las colas de cuarentena correspondientes.

**¿Dónde revisar su implementación?**

- `RabbitMQConfig.java`: configuración de exchanges, colas y bindings.
- `GuestNotificationConsumer.java`: procesamiento de notificaciones para huéspedes.
- `ReceptionNotificationConsumer.java`: procesamiento de notificaciones para recepción.
- `BrokerGateway.java`: publicación de mensajes.
- `config/rabbit-topology.properties`: catálogo centralizado de topología.

### Apache Kafka: eventos y auditoría

Kafka se utiliza para registrar y distribuir eventos importantes del sistema.

Por ejemplo, cuando se crea o confirma una reserva:

1. El backend genera un evento.
2. El evento se registra mediante el mecanismo Outbox.
3. El productor publica el evento en Kafka.
4. Kafka lo almacena en un topic.
5. Los consumidores reciben y procesan los eventos.
6. Los errores de procesamiento pueden derivarse a una Dead Letter Topic (DLT).

**Topics principales:**

| Topic | Propósito |
|---|---|
| `andesstay.events` | Eventos generales del sistema. |
| `andesstay.reservations` | Eventos relacionados con las reservas. |
| `andesstay.audit` | Eventos de auditoría. |

Los topics de reservas y auditoría utilizan particiones para distribuir el procesamiento. También tienen políticas de retención para conservar los eventos durante un período definido.

**¿Qué es Outbox?**

El patrón Outbox permite guardar el evento pendiente en la base de datos antes de intentar publicarlo en Kafka. Un proceso posterior consulta los eventos pendientes y gestiona su envío.

Esto ayuda a evitar que un cambio de negocio quede sin su correspondiente evento cuando ocurre un problema durante la publicación.

**¿Qué es DLT?**

Una Dead Letter Topic recibe los mensajes que no pudieron procesarse después de los reintentos configurados. Permite conservarlos para su posterior análisis sin bloquear indefinidamente el procesamiento normal.

**¿Dónde revisar su implementación?**

- `MessagingConfig.java`: configuración de topics y tratamiento de errores.
- `KafkaProducer.java`: publicación de eventos.
- `KafkaConsumer.java`: consumo de eventos.
- `KafkaEventProcessingService.java`: procesamiento de eventos.
- `OutboxService.java`: gestión de eventos pendientes.
- `ReservationEventDTO.java`: estructura de eventos de reservas.
- `AuditEventDTO.java`: estructura de eventos de auditoría.

## 3. Administradores de mensajería

El proyecto incluye servicios administrativos independientes:

- **Rabbit Admin:** permite administrar y consultar recursos de RabbitMQ.
- **Kafka Admin:** permite administrar topics de Kafka.

Son herramientas de administración; no reemplazan a los brokers.

Para iniciar los servicios opcionales:

```powershell
.\scripts\export-rabbit-topology.cmd

docker compose --env-file .env -f docker/docker-compose.yml --profile operations up --build -d --wait
```

Direcciones locales:

| Servicio | Dirección |
|---|---|
| RabbitMQ Management | http://localhost:15672 |
| Rabbit Admin | http://localhost:8091 |
| Kafka Admin | http://localhost:8092 |

Las credenciales de RabbitMQ se encuentran en el archivo `.env` local. Las API administrativas requieren autorización con el rol correspondiente.

## 4. Ejecutar pruebas

Para ejecutar las pruebas habituales:

```powershell
.\scripts\test-local.cmd
```

Para probar Rabbit Admin con un broker real:

```powershell
.\scripts\test-rabbit-admin.cmd
```

Para ejecutar las pruebas de integración con MySQL y los brokers reales, se necesita una copia local del respaldo de base de datos en la ubicación indicada por el script:

```powershell
.\scripts\test-mysql.cmd
```

Las pruebas y sus requisitos adicionales están documentados en `TESTS.md`.

## 5. Documentación adicional

El directorio `docs/` contiene información más detallada sobre la implementación:

- [Eventos confiables y mensajería](docs/05-eventos-confiables.md)
- [RabbitMQ: reservas y notificaciones](docs/10-rabbitmq-reservas.md)
- [RabbitMQ: DLQ y reintentos](docs/11-rabbitmq-dlq-retencion.md)
- [RabbitMQ: topología centralizada](docs/12-topologia-rabbitmq-centralizada.md)
- [RabbitMQ: consumidores](docs/13-consumidores-por-dominio.md)
- [RabbitMQ: ACK y clasificación de errores](docs/14-ack-manual-y-clasificacion-de-errores.md)
- [Rabbit Admin](docs/15-rabbit-admin-crud.md)
- [Kafka y RabbitMQ: evidencias del proyecto](docs/21-evidencias-ep3.md)

## 6. Configuración y seguridad

- `.env.example` contiene la plantilla de configuración.
- `.env` contiene la configuración privada del entorno local y no debe publicarse.
- No publiques contraseñas, tokens, certificados ni respaldos de bases de datos.
- El entorno local utiliza un nodo Kafka y un broker RabbitMQ; no representa por sí solo una configuración de alta disponibilidad para producción.

---

**AndesStay:** gestión de reservas, procesamiento de eventos y notificaciones mediante una arquitectura basada en microservicios y mensajería.
