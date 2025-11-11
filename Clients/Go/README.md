# SynergyNetClient

## Load test tool

Se agregó un ejecutable para pruebas de balanceo y tolerancia a fallas en `cmd/loadtest`.

Cubre:

- Múltiples clientes workers (mismo grupo) que atienden eventos y responden estados.
- Clientes productores que envían: eventos simples (echo), trabajo (work) y arrays (bulk).
- Desconexiones aleatorias de algunos workers durante la prueba.
- Estrés con alto volumen y concurrencia configurables.

Cómo construir y ejecutar (requiere que el servidor esté levantado con TLS en el puerto 443):

```bash
cd SynergyNetClient
go build ./cmd/loadtest
./loadtest -host localhost -port 443 \
	-group WORKER -workers 3 -producers 1 -events 1000 -concurrency 50 \
	-drop=true -dropAfter 5s -timeout 5s -payload 128 -arrayEvents 10
```

Flags principales:

- -group: nombre del grupo de workers (los workers se registran con este nombre para que el servidor los balancee por latencia).
- -workers: cantidad de clientes en el grupo.
- -producers: cantidad de productores que envían eventos.
- -events: eventos totales a enviar entre todos los productores.
- -concurrency: máximos eventos en vuelo por productor.
- -drop: simular caídas de algunos workers en medio de la prueba.
- -arrayEvents: registra handlers adicionales en los workers para simular que soportan muchos tipos de eventos.

Métricas impresas al final:

- enviados, confirmados (acks), fallidos/timeout, latencia promedio y throughput.
