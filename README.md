# Dispatch Consumer - Microservicio Reactivo

Microservicio consumer de eventos que procesa solicitudes de reemplazo de tarjetas bancarias con persistencia reactiva en MongoDB, cache en Redis y procesamiento via Apache Kafka.

## Requisitos Previos

- Docker Desktop o Docker Engine instalado
- Docker Compose v2.0+
- Puerto 8086 disponible (aplicación)
- Puertos 9092, 27017, 6379, 8081 disponibles (servicios)

## Contenedores Necesarios

El proyecto utiliza los siguientes contenedores (definidos en `docker-compose.yml`):

| Servicio | Imagen | Puerto | Descripción |
|----------|--------|--------|------------|
| Kafka | apache/kafka:4.2.0 | 9092 | Event streaming broker |
| Schema Registry | confluentinc/cp-schema-registry:8.1.0 | 8081 | Registry para schemas Avro |
| MongoDB | mongo:latest | 27017 | Base de datos de persistencia |
| Redis | redis:8-alpine | 6379 | Cache de snapshots |
| Kafka UI | provectuslabs/kafka-ui:v0.7.2 | 8082 | Dashboard Kafka (opcional) |
| dispatch-consumer | dispatch-consumer:latest | 8086 | Aplicación (build local) |

## Inicio Rápido

### 1. Construir la imagen Docker

```bash
# Build la imagen con tag 'latest'
docker build -t dispatch-consumer:latest .
```

### 2. Levantar todos los contenedores

```bash
# Crear e iniciar todos los servicios
docker-compose up -d

# Ver logs en tiempo real
docker-compose logs -f dispatch-consumer

# Ver todos los logs
docker-compose logs -f
```

### 3. Verificar que está funcionando

```bash
# Healthcheck del microservicio
curl http://localhost:8086/api/health

# Respuesta esperada: OK
```

## Endpoints de Consulta

### GET /api/events
Consulta un evento por requestId con estrategia cache-aside (Redis -> MongoDB)

```bash
curl -X GET "http://localhost:8086/api/events?requestId=REQ-20260617-101"
```

Respuesta exitosa (200):
```json
{
  "requestId": "REQ-20260617-101",
  "customerId": "CUS-987654",
  "branchCode": "BR001",
  "deliveryAddress": "Av. Javier Prado Este 1234, San Isidro, Lima",
  "status": "DISPATCHED_CACHE",
  "attemptNumber": 2,
  "receivedAt": 1781747664974,
  "processedAt": 1781747665064
}
```

Respuesta no encontrado (404): RequestId no existe

### GET /api/health
Healthcheck del servicio

```bash
curl http://localhost:8086/api/health
```

## Flujo de Procesamiento

### Primer Intento
1. Evento llega vía Kafka (topic: `bank.card.replacements.v1`)
2. Si requestId no existe en MongoDB:
   - Persiste con status=DISPATCHED, attemptNumber=1
   - Guarda snapshot en Redis (card:event:{requestId})
   - Confirma al broker Kafka

### Segundo Intento
3. Mismo requestId llega de nuevo:
   - Existe en MongoDB -> obtiene snapshot de Redis
   - Enriquece el evento
   - Actualiza MongoDB con status=DISPATCHED_CACHE, attemptNumber=2
   - Actualiza snapshot en Redis
   - Confirma al broker

### Manejo de Errores
- RequestId inválido -> Envía a DLT (Dead Letter Topic)
- Error en Redis -> Continúa (permite degradación)
- Error en MongoDB -> Reintenta

## Cache-Aside Pattern

La estrategia cache-aside optimiza consultas:

1. GET /api/events intenta Redis primero
2. Si Redis tiene snapshot con status=DISPATCHED_CACHE:
   - Retorna inmediatamente (sin golpear MongoDB)
3. Si no existe o es inválido:
   - Consulta MongoDB como fallback
4. Ventaja: Reduce carga en MongoDB significativamente

## Configuración de Variables de Entorno

Se pueden sobrescribir en `docker-compose.yml` o al ejecutar:

```bash
# Ejemplo: cambiar puerto de MongoDB
docker-compose up -e MONGO_URI=mongodb://mi-mongo:27017/cardsdb -d
```

| Variable | Default | Descripción |
|----------|---------|------------|
| KAFKA_BOOTSTRAP_SERVERS | kafka:9092 | Bootstrap de Kafka |
| SCHEMA_REGISTRY_URL | http://schema-registry:8081 | URL del Schema Registry |
| MONGO_URI | mongodb://mongodb:27017/cardsdb | URI de MongoDB |
| REDIS_HOST | redis | Host de Redis |
| REDIS_PORT | 6379 | Puerto de Redis |
| SERVER_PORT | 8086 | Puerto de la aplicación |

## Monitoreo

### Kafka UI
Accede a http://localhost:8082 para visualizar topics y mensajes

### Logs de la Aplicación
```bash
# Solo dispatch-consumer
docker-compose logs dispatch-consumer

# Con seguimiento en tiempo real
docker-compose logs -f dispatch-consumer

# Filtrar por palabras clave
docker-compose logs dispatch-consumer | grep "Snapshot\|DISPATCHED"
```

### Verificar datos en MongoDB
```bash
# Acceder a mongo shell
docker exec -it mongodb mongosh

# Ver colecciones
show databases
use cardsdb
show collections

# Consultar documentos
db.card_replacements.find().pretty()
```

### Verificar datos en Redis
```bash
# Acceder a redis cli
docker exec -it redis redis-cli

# Ver claves
KEYS card:event:*

# Ver valor del snapshot
GET card:event:REQ-20260617-101
```

## Detener Servicios

```bash
# Parar todos los contenedores (sin eliminar volúmenes)
docker-compose stop

# Parar y eliminar contenedores
docker-compose down

# Parar y eliminar todo (incluyendo volúmenes)
docker-compose down -v
```

## Troubleshooting

### La aplicación no conecta a Kafka
```bash
# Verificar que Kafka está corriendo
docker-compose ps

# Ver logs de Kafka
docker-compose logs kafka
```

### Los snapshots no se guardan en Redis
```bash
# Verificar conexión a Redis
docker exec -it redis redis-cli ping
# Debe responder: PONG

# Ver todas las claves
docker exec -it redis redis-cli KEYS "*"
```

### MongoDB rechaza conexiones
```bash
# Verificar logs de MongoDB
docker-compose logs mongodb

# Verificar puerto
docker-compose ps mongodb
```

## Stack Tecnológico

- Spring Boot 3.5.5 (Java 17)
- Reactor Kafka - Consumer reactivo
- MongoDB - Persistencia reactiva
- Redis - Cache reactivo
- RxJava3 - Programación reactiva
- Apache Avro - Serialización de eventos
- Micrometer - Métricas

## Notas

- Los snapshots en Redis tienen TTL de 24 horas
- Los volúmenes de Docker persisten datos entre reinicios
- La aplicación requiere los 4 servicios para funcionar correctamente
- El --noauth en MongoDB es solo para desarrollo (NO para producción)
