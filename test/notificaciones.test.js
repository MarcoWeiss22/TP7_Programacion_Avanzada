
import test from "node:test";
import assert from "node:assert/strict";
import { obtenerNotificacion } from "../servicios/notificaciones/lib/deduplicacion.js";

function crearColeccionFalsa() {
    const registros = new Map();

    return {
        registros,

        async findOneAndUpdate(filtro, actualizacion) {
            let registro = registros.get(filtro.eventoOrigenId);

            if (!registro) {
                registro = { ...actualizacion.$setOnInsert };
                registros.set(filtro.eventoOrigenId, registro);
            }

            return { value: { ...registro } };
        },
    };
}

function dependencias(notificaciones) {
    let siguienteId = 1;

    return {
        notificaciones,
        generarId: () => `notificacion-${siguienteId++}`,
        obtenerFecha: () => "2026-10-21T12:00:00.000Z",
    };
}

test("el mismo evento no crea dos notificaciones", async () => {
    const coleccion = crearColeccionFalsa();
    const deps = dependencias(coleccion);
    const evento = {
        eventId: "evento-123",
        shipmentId: "envio-456",
        tipo: "ENTREGA_FALLIDA",
        motivo: "AUSENTE",
    };

    const primera = await obtenerNotificacion({ ...deps, evento });
    const segunda = await obtenerNotificacion({ ...deps, evento });

    assert.equal(coleccion.registros.size, 1);
    assert.equal(primera.eventId, segunda.eventId);
    assert.equal(primera.eventoOrigenId, segunda.eventoOrigenId);
});

test("un evento diferente crea otra notificación", async () => {
    const coleccion = crearColeccionFalsa();
    const deps = dependencias(coleccion);

    await obtenerNotificacion({
        ...deps,
        evento: { eventId: "evento-1", shipmentId: "envio-1" },
    });

    await obtenerNotificacion({
        ...deps,
        evento: { eventId: "evento-2", shipmentId: "envio-2" },
    });

    assert.equal(coleccion.registros.size, 2);
});

test("rechaza eventos sin identificadores válidos", async () => {
    const coleccion = crearColeccionFalsa();

    await assert.rejects(
        obtenerNotificacion({
            ...dependencias(coleccion),
            evento: { eventId: "evento-1", shipmentId: " " },
        }),
        /identificadores válidos/
    );

    assert.equal(coleccion.registros.size, 0);
});
