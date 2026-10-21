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

        const envios = await db
            .collection("envios")
            .find(
                {},
                {
                    projection: {
                        _id: 0,
                        shipmentId: 1,
                        actualizadoEn: 1,
                        creadoEn: 1,
                        estadoActual: 1,
                        hubId: 1,
                        zonaId: 1,
                        repartidorId: 1,
                        intentos: 1,
                        historial: 1,
                    },
                }
            )
            .sort({ actualizadoEn: -1 })
            .limit(100)
            .toArray();

        return NextResponse.json(envios);
    } catch (error) {
        console.error("Error al consultar los envíos:", error);

        return NextResponse.json(
            { error: "No se pudieron consultar los envíos" },
            { status: 500 }
        );
    }
}