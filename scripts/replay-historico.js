import { Kafka } from "kafkajs";
import { MongoClient } from "mongodb";

const TOPICS = [
    "envios.estados",
    "envios.promesas",
    "envios.reprogramaciones",
];

const kafka = new Kafka({
    clientId: "rutasur-replay-diagnostico",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: `rutasur-replay-diagnostico-${Date.now()}`,
});
const admin = kafka.admin();

const mongo = new MongoClient(
    process.env.MONGO_URL ||
    "mongodb://localhost:27017/?replicaSet=rs0"
);

function fechaValida(valor) {
    if (!valor) return null;
    const fecha = new Date(valor);
    return Number.isNaN(fecha.getTime()) ? null : fecha;
}

function validarMensaje(topic, evento) {
    if (
        !evento ||
        typeof evento !== "object" ||
        Array.isArray(evento) ||
        typeof evento.shipmentId !== "string" ||
        !evento.shipmentId.trim()
    ) return "ESQUEMA_INVALIDO";

    if (topic === "envios.estados") {
        if (
            typeof evento.eventId !== "string" ||
            !evento.eventId.trim() ||
            typeof evento.tipo !== "string" ||
            !evento.tipo.trim()
        ) return "EVENTO_ESTADO_INCOMPLETO";
    }

    if (topic === "envios.promesas" &&
        !fechaValida(evento.fechaPrometida)) {
        return "PROMESA_INVALIDA";
    }

    if (topic === "envios.reprogramaciones" &&
        (typeof evento.eventId !== "string" ||
         !evento.eventId.trim())) {
        return "REPROGRAMACION_SIN_EVENT_ID";
    }

    return null;
}

function calcularMetricas(estados, promesas) {
    const porEnvio = new Map();

    for (const evento of estados) {
        if (!porEnvio.has(evento.shipmentId)) {
            porEnvio.set(evento.shipmentId, []);
        }
        porEnvio.get(evento.shipmentId).push(evento);
    }

    for (const lista of porEnvio.values()) {
        lista.sort((a, b) => {
            const diferencia = (a.secuencia ?? 0) - (b.secuencia ?? 0);
            if (diferencia !== 0) return diferencia;
            return (a.ocurridoEn || "").localeCompare(b.ocurridoEn || "");
        });
    }

    const promesasPorEnvio = new Map(
        promesas.map((p) => [p.shipmentId, p])
    );

    let enviosEntregados = 0;
    let entregadosPrimerIntento = 0;

    for (const lista of porEnvio.values()) {
        const entrega = lista.find((e) => e.tipo === "ENTREGADO");
        if (!entrega) continue;

        enviosEntregados++;

        const tuvoFallo = lista.some((e) =>
            e.tipo === "ENTREGA_FALLIDA" &&
            fechaValida(e.ocurridoEn) &&
            fechaValida(entrega.ocurridoEn) &&
            fechaValida(e.ocurridoEn) <= fechaValida(entrega.ocurridoEn)
        );

        if (!tuvoFallo) entregadosPrimerIntento++;
    }

    const cumplimiento = new Map();

    for (const [shipmentId, lista] of porEnvio.entries()) {
        const entrega = [...lista].reverse().find(
            (e) => e.tipo === "ENTREGADO"
        );
        if (!entrega) continue;

        const promesa = promesasPorEnvio.get(shipmentId);
        const fechaEntrega = fechaValida(entrega.ocurridoEn);
        const fechaPrometida = fechaValida(promesa?.fechaPrometida);
        if (!fechaEntrega || !fechaPrometida) continue;

        const hubId = entrega.hubId || promesa?.hubId || "SIN_HUB";
        const dia = fechaEntrega.toISOString().slice(0, 10);
        const clave = `${hubId}|${dia}`;

        const dato = cumplimiento.get(clave) || {
            hubId,
            dia,
            totalEntregados: 0,
            entregadosATiempo: 0,
        };

        dato.totalEntregados++;
        if (fechaEntrega <= fechaPrometida) dato.entregadosATiempo++;
        cumplimiento.set(clave, dato);
    }

    const cumplimientoSla = [...cumplimiento.values()].map((d) => ({
        ...d,
        porcentajeCumplimiento: d.totalEntregados === 0
            ? 0
            : Number(
                ((d.entregadosATiempo / d.totalEntregados) * 100).toFixed(2)
            ),
    })).sort((a, b) =>
        `${a.hubId}|${a.dia}`.localeCompare(`${b.hubId}|${b.dia}`)
    );

    const fallos = new Map();

    for (const evento of estados) {
        if (evento.tipo !== "ENTREGA_FALLIDA") continue;
        const zonaId = evento.zonaId || "SIN_ZONA";
        const motivo = evento.motivo || "SIN_MOTIVO";
        const clave = `${zonaId}|${motivo}`;
        fallos.set(clave, (fallos.get(clave) || 0) + 1);
    }

    const fallosPorZonaMotivo = [...fallos.entries()].map(
        ([clave, cantidad]) => {
            const [zonaId, motivo] = clave.split("|");
            return { zonaId, motivo, cantidad };
        }
    ).sort((a, b) =>
        `${a.zonaId}|${a.motivo}`.localeCompare(`${b.zonaId}|${b.motivo}`)
    );

    const tiempos = new Map();

    for (const lista of porEnvio.values()) {
        const ingreso = lista.find((e) => e.tipo === "INGRESADO_HUB");
        const salida = lista.find((e) => e.tipo === "EN_REPARTO");
        if (!ingreso || !salida) continue;

        const fechaIngreso = fechaValida(ingreso.ocurridoEn);
        const fechaSalida = fechaValida(salida.ocurridoEn);

        if (!fechaIngreso || !fechaSalida || fechaSalida < fechaIngreso) {
            continue;
        }

        const hubId = ingreso.hubId || salida.hubId || "SIN_HUB";
        const dato = tiempos.get(hubId) || {
            hubId,
            cantidadEnvios: 0,
            totalMinutos: 0,
        };

        dato.cantidadEnvios++;
        dato.totalMinutos +=
            (fechaSalida.getTime() - fechaIngreso.getTime()) / 60000;
        tiempos.set(hubId, dato);
    }

    const tiempoPromedioHub = [...tiempos.values()].map((d) => ({
        hubId: d.hubId,
        cantidadEnvios: d.cantidadEnvios,
        promedioMinutos: d.cantidadEnvios === 0
            ? 0
            : Number((d.totalMinutos / d.cantidadEnvios).toFixed(2)),
    })).sort((a, b) => a.hubId.localeCompare(b.hubId));

    return {
        entregaPrimerIntento: {
            enviosEntregados,
            entregadosPrimerIntento,
            porcentaje: enviosEntregados === 0
                ? 0
                : Number(
                    ((entregadosPrimerIntento / enviosEntregados) * 100)
                        .toFixed(2)
                ),
        },
        fallosPorZonaMotivo,
        cumplimientoSla,
        tiempoPromedioHub,
    };
}

function comparar(actual, reconstruido) {
    const ordenar = (lista, clave) =>
        [...(lista || [])].sort((a, b) =>
            clave(a).localeCompare(clave(b))
        );

    const normalizar = (indicador, valor) => {
        if (!valor) return valor;

        if (indicador === "fallosPorZonaMotivo") {
            return ordenar(valor, x => `${x.zonaId}|${x.motivo}`);
        }

        if (indicador === "cumplimientoSla") {
            return ordenar(valor, x => `${x.hubId}|${x.dia}`);
        }

        if (indicador === "tiempoPromedioHub") {
            return ordenar(valor, x => x.hubId);
        }

        return valor;
    };

    const indicadores = [
        "entregaPrimerIntento",
        "fallosPorZonaMotivo",
        "cumplimientoSla",
        "tiempoPromedioHub",
    ];

    return Object.fromEntries(indicadores.map(indicador => [
        indicador,
        JSON.stringify(normalizar(indicador, actual?.[indicador])) ===
        JSON.stringify(normalizar(indicador, reconstruido[indicador]))
    ]));
}
async function main() {
    const resumen = Object.fromEntries(TOPICS.map((topic) => [
        topic,
        { leidos: 0, validos: 0, invalidos: 0, errores: {} },
    ]));

    const idsEstados = new Set();
    const idsReprogramaciones = new Set();
    const estados = [];
    const promesasPorEnvio = new Map();
    const reprogramaciones = new Map();

    let esperados = 0;
    let recibidos = 0;

    try {
        await admin.connect();

        for (const topic of TOPICS) {
            const particiones = await admin.fetchTopicOffsets(topic);
            for (const p of particiones) {
                const low = Number(p.low ?? 0);
                const high = Number(p.high ?? p.offset);
                if (!Number.isFinite(low) || !Number.isFinite(high)) {
                    throw new Error(`No se pudieron leer offsets de ${topic}`);
                }
                esperados += Math.max(0, high - low);
            }
        }

        console.log(`Mensajes esperados al iniciar: ${esperados}`);

        await consumer.connect();
        await consumer.subscribe({ topics: TOPICS, fromBeginning: true });

        let temporizador;
        const finalizado = new Promise((resolve, reject) => {
            temporizador = setTimeout(() => reject(
                new Error(`Timeout: leídos ${recibidos} de ${esperados}`)
            ), 120000);

            consumer.run({
                autoCommit: false,
                eachMessage: async ({ topic, message }) => {
                    const r = resumen[topic];
                    r.leidos++;
                    recibidos++;

                    let evento;
                    let error = null;

                    try {
                        evento = JSON.parse(message.value?.toString() ?? "");
                    } catch {
                        error = "JSON_MALFORMADO";
                    }

                    if (!error) error = validarMensaje(topic, evento);

                    if (error) {
                        r.invalidos++;
                        r.errores[error] = (r.errores[error] || 0) + 1;
                    } else {
                        r.validos++;

                        if (topic === "envios.estados") {
                            if (idsEstados.has(evento.eventId)) {
                                // La analítica existente conserva la primera
                                // aparición de cada eventId.
                            } else {
                                idsEstados.add(evento.eventId);
                                estados.push({
                                    eventId: evento.eventId,
                                    shipmentId: evento.shipmentId,
                                    tipo: evento.tipo,
                                    motivo: evento.motivo || null,
                                    hubId: evento.hubId || null,
                                    zonaId: evento.zonaId || null,
                                    ocurridoEn: fechaValida(evento.ocurridoEn)
                                        ?.toISOString() ?? null,
                                    secuencia: evento.secuencia ?? null,
                                });
                            }
                        } else if (topic === "envios.promesas") {
                            if (fechaValida(evento.fechaPrometida)) {
                                promesasPorEnvio.set(evento.shipmentId, {
                                    shipmentId: evento.shipmentId,
                                    fechaPrometida:
                                        fechaValida(evento.fechaPrometida).toISOString(),
                                    hubId: evento.hubId || null,
                                    zonaId: evento.zonaId || null,
                                });
                            }
                        } else if (topic === "envios.reprogramaciones") {
                            if (!idsReprogramaciones.has(evento.eventId)) {
                                idsReprogramaciones.add(evento.eventId);
                                reprogramaciones.set(evento.eventId, evento);
                            }
                        }
                    }

                    if (recibidos >= esperados) {
                        clearTimeout(temporizador);
                        resolve();
                    }
                },
            }).catch((error) => {
                clearTimeout(temporizador);
                reject(error);
            });
        });

        if (esperados > 0) await finalizado;
        clearTimeout(temporizador);
        await consumer.stop();

        if (recibidos !== esperados) {
            throw new Error(`Lectura incompleta: ${recibidos}/${esperados}`);
        }

        await mongo.connect();
        const db = mongo.db("rutasur");
        const actual = await db.collection("metricas_analitica")
            .findOne({ _id: "indicadores_actuales" });

        const reconstruido = calcularMetricas(
            estados,
            [...promesasPorEnvio.values()]
        );

        console.log("\n=== RESUMEN DE MENSAJES ===");
        console.log(JSON.stringify(resumen, null, 2));
        console.log(`\nTotal leído: ${recibidos}/${esperados}`);
        console.log(`Estados únicos: ${estados.length}`);
        console.log(`Promesas por envío: ${promesasPorEnvio.size}`);
        console.log(`Reprogramaciones únicas: ${reprogramaciones.size}`);

        console.log("\n=== MÉTRICAS RECONSTRUIDAS EN MEMORIA ===");
        console.log(JSON.stringify(reconstruido, null, 2));

        console.log("\n=== MÉTRICAS ACTUALES ===");
        console.log(JSON.stringify(actual, null, 2));

        console.log("\n=== COMPARACIÓN ===");
        console.log(JSON.stringify(comparar(actual, reconstruido), null, 2));

        console.log(
            "\nDiagnóstico terminado: no se escribieron datos en MongoDB ni se publicaron mensajes."
        );
    } finally {
        await consumer.disconnect().catch(() => {});
        await admin.disconnect().catch(() => {});
        await mongo.close().catch(() => {});
    }
}

main().catch((error) => {
    console.error("Falló el diagnóstico:", error.message);
    process.exitCode = 1;
});

