import { NextResponse } from "next/server";
import { MongoClient } from "mongodb";


const uri = process.env.MONGODB_URI;
if (!uri) throw new Error("Falta configurar MONGODB_URI en .env.local");

const globalMongo = globalThis as typeof globalThis & {
  mongoClientPromise?: Promise<MongoClient>;
};

const clientPromise =
  globalMongo.mongoClientPromise ?? new MongoClient(uri).connect();

if (process.env.NODE_ENV !== "production") {
  globalMongo.mongoClientPromise = clientPromise;
}

export async function GET(
  _request: Request,
  context: { params: Promise<{ shipmentId: string }> }
) {
  try {
    const { shipmentId } = await context.params;
    const client = await clientPromise;

    const envio = await client.db("rutasur").collection("envios").findOne(
      { shipmentId },
      {
        projection: {
          _id: 0,
          shipmentId: 1,
          estadoActual: 1,
          creadoEn: 1,
          actualizadoEn: 1,
          historial: 1,
        },
      }
    );

    if (!envio) {
      return NextResponse.json(
        { error: "No se encontró el envío solicitado" },
        { status: 404 }
      );
    }

    const historial = Array.isArray(envio.historial)
      ? envio.historial.map((item: Record<string, unknown>) => ({
          tipo: item.tipo,
          ocurridoEn: item.ocurridoEn,
        }))
      : [];

    return NextResponse.json({
      shipmentId: envio.shipmentId,
      estadoActual: envio.estadoActual,
      creadoEn: envio.creadoEn,
      actualizadoEn: envio.actualizadoEn,
      historial,
    }, {
      headers: { "Cache-Control": "no-store" }
    });
  } catch (error) {
    console.error("Error al consultar seguimiento público:", error);
    return NextResponse.json(
      { error: "No se pudo consultar el seguimiento" },
      { status: 500 }
    );
  }
}