import { NextResponse } from "next/server";
import { MongoClient } from "mongodb";

const uri = process.env.MONGODB_URI;

if (!uri) {
    throw new Error("Falta configurar MONGODB_URI en .env.local");
}

const globalConMongo = globalThis as typeof globalThis & {
    mongoClientPromise?: Promise<MongoClient>;
};

const clientPromise =
    globalConMongo.mongoClientPromise ?? new MongoClient(uri).connect();

if (process.env.NODE_ENV !== "production") {
    globalConMongo.mongoClientPromise = clientPromise;
}

export async function GET() {
    try {
        const client = await clientPromise;
        const db = client.db("rutasur");

        const alertas = await db
            .collection("alertas_sla")
            .find(
                {},
                {
                    projection: {
                        _id: 0,
                        alertaId: 1,
                        shipmentId: 1,
                        regla: 1,
                        estado: 1,
                        prioridad: 1,
                        descripcion: 1,
                        fechaPrometida: 1,
                        creadaEn: 1,
                        actualizadaEn: 1,
                        cerradaEn: 1,
                        motivoCierre: 1,
                        hubId: 1,
                        zonaId: 1,
                    },
                }
            )
            .sort({ creadaEn: -1 })
            .limit(100)
            .toArray();

        return NextResponse.json(alertas);
    } catch (error) {
        console.error("Error al consultar las alertas SLA:", error);

        return NextResponse.json(
            { error: "No se pudieron consultar las alertas SLA" },
            { status: 500 }
        );
    }
}