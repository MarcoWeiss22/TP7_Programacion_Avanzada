
export async function obtenerNotificacion({
    notificaciones,
    evento,
    generarId,
    obtenerFecha,
}) {
    if (
        typeof evento?.eventId !== "string" ||
        !evento.eventId.trim() ||
        typeof evento?.shipmentId !== "string" ||
        !evento.shipmentId.trim()
    ) {
        throw new Error("Evento sin identificadores válidos");
    }

    const resultado = await notificaciones.findOneAndUpdate(
        { eventoOrigenId: evento.eventId },
        {
            $setOnInsert: {
                eventId: generarId(),
                eventoOrigenId: evento.eventId,
                shipmentId: evento.shipmentId,
                tipo: "NOTIFICACION_ENTREGA_FALLIDA",
                motivo: evento.motivo ?? "NO_ESPECIFICADO",
                mensaje:
                    `El envío ${evento.shipmentId} no pudo entregarse. ` +
                    "Contacte al operador para conocer las opciones disponibles.",
                ocurridoEn: obtenerFecha(),
                version: 1,
                publicado: false,
            },
        },
        {
            upsert: true,
            returnDocument: "after",
        }
    );

    return resultado?.value ?? resultado;
}
