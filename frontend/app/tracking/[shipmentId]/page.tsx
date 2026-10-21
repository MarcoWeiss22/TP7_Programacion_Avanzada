"use client";

import { Suspense, useEffect, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";

type Evento = { tipo?: string; ocurridoEn?: string };
type Seguimiento = {
  shipmentId: string;
  estadoActual?: string;
  actualizadoEn?: string;
  historial: Evento[];
};

function ContenidoSeguimiento() {
  const { shipmentId } = useParams<{ shipmentId: string }>();
  const [envio, setEnvio] = useState<Seguimiento | null>(null);
  const [error, setError] = useState("");
  const [cargando, setCargando] = useState(true);

  useEffect(() => {
    let activo = true;

    async function consultar() {
      try {
        const respuesta = await fetch(
          `/api/tracking/${encodeURIComponent(shipmentId)}`,
          { cache: "no-store" }
        );
        const datos = await respuesta.json();
        if (!respuesta.ok) throw new Error(datos.error || "No se encontró el envío");
        if (activo) {
          setEnvio(datos);
          setError("");
        }
      } catch (e) {
        if (activo) setError(e instanceof Error ? e.message : "Error de consulta");
      } finally {
        if (activo) setCargando(false);
      }
    }

    consultar();
    const intervalo = setInterval(consultar, 10000);
    return () => {
      activo = false;
      clearInterval(intervalo);
    };
  }, [shipmentId]);

  const fecha = (valor?: string) => {
    if (!valor) return "No disponible";
    const parsed = new Date(valor);
    return Number.isNaN(parsed.getTime()) ? valor : parsed.toLocaleString("es-AR");
  };

  return (
    <main style={{ maxWidth: 760, margin: "40px auto", padding: 24 }}>
      <Link href="/">Volver a RutaSur</Link>
      <h1 style={{ marginTop: 24 }}>Seguimiento de envío</h1>
      <p>Identificador: <strong>{shipmentId}</strong></p>
      {cargando && <p>Consultando el estado del envío...</p>}
      {error && <p role="alert">{error}</p>}
      {envio && (
        <>
          <section style={{ padding: 20, border: "1px solid #8885", borderRadius: 12 }}>
            <p>Estado actual</p>
            <h2>{envio.estadoActual || "Sin estado registrado"}</h2>
            <p>Última actualización: {fecha(envio.actualizadoEn)}</p>
          </section>
          <h2 style={{ marginTop: 28 }}>Historial de seguimiento</h2>
          {envio.historial.length === 0 ? (
            <p>Todavía no hay movimientos registrados.</p>
          ) : (
            <ol>
              {envio.historial.map((evento, indice) => (
                <li key={`${evento.tipo}-${evento.ocurridoEn}-${indice}`} style={{ marginBottom: 16 }}>
                  <strong>{evento.tipo || "Movimiento"}</strong>
                  <br />
                  <span>{fecha(evento.ocurridoEn)}</span>
                </li>
              ))}
            </ol>
          )}
          <p style={{ fontSize: 13, opacity: 0.7 }}>
            Se muestran únicamente el estado y los movimientos básicos del envío.
          </p>
        </>
      )}
    </main>
  );
}

export default function SeguimientoPublico() {
  return (
    <Suspense fallback={<main style={{ padding: 24 }}>Cargando seguimiento...</main>}>
      <ContenidoSeguimiento />
    </Suspense>
  );
}