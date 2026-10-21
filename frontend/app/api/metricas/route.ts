
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
    globalConMongo.mongoClientPromise ??
    new MongoClient(uri).connect();

if (process.env.NODE_ENV !== "production") {
    globalConMongo.mongoClientPromise = clientPromise;
}

export async function GET() {
    try {
        const client = await clientPromise;
        const db = client.db("rutasur");

        const metricas = await db
            .collection<{ _id: string }>("metricas_analitica")
            .findOne({ _id: "indicadores_actuales" });

        if (!metricas) {
            return NextResponse.json(
                { error: "Todavía no hay métricas calculadas" },
                { status: 404 }
            );
        }

        return NextResponse.json(metricas);
    } catch (error) {
        console.error("Error al consultar las métricas:", error);

        return NextResponse.json(
            { error: "No se pudieron consultar las métricas" },
            { status: 500 }
        );
    }
}
