
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type EventoHistorial = {
  eventId: string;
  tipo: string;
  ocurridoEn: string;
  secuencia: number;
};

type Envio = {
  shipmentId: string;
  actualizadoEn: string;
  creadoEn?: string;
  estadoActual: string;
  hubId: string;
  intentos: number;
  repartidorId: string | null;
  zonaId: string;
  historial?: EventoHistorial[];
};

type AlertaSLA = {
  alertaId: string;
  shipmentId: string;
  regla: string;
  estado: string;
  prioridad: string;
  descripcion: string;
  fechaPrometida?: string;
  creadaEn?: string;
  actualizadaEn?: string;
  cerradaEn?: string;
  motivoCierre?: string;
  hubId?: string;
  zonaId?: string;
};

type Metricas = Record<string, unknown>;

const MENU = [
  "Resumen",
  "Torre de control",
  "Alertas SLA",
  "Seguimiento",
  "Analítica",
  "Demostración",
];

const panel =
  "rounded-2xl border border-slate-800 bg-[#111c30] p-5";

const inputStyle =
  "w-full rounded-xl border border-slate-700 bg-[#0b1425] px-4 py-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-emerald-500";

function fecha(valor?: string) {
  if (!valor) return "Sin fecha";

  const date = new Date(valor);

  if (Number.isNaN(date.getTime())) return valor;

  return date.toLocaleString("es-AR");
}

function normalizar(valor: string) {
  return valor
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function prioridadClase(prioridad: string) {
  const p = normalizar(prioridad);

  if (p === "critica") {
    return "bg-red-400/10 text-red-300 border-red-400/20";
  }

  if (p === "alta") {
    return "bg-orange-400/10 text-orange-300 border-orange-400/20";
  }

  return "bg-amber-400/10 text-amber-300 border-amber-400/20";
}

function estadoClase(estado: string) {
  if (normalizar(estado) === "abierta") {
    return "bg-amber-400/10 text-amber-300";
  }

  return "bg-emerald-400/10 text-emerald-300";
}

function etiquetaEstado(estado: string) {
  return estado.replaceAll("_", " ");
}

function Badge({
  children,
  clase = "bg-slate-700/50 text-slate-300",
}: {
  children: React.ReactNode;
  clase?: string;
}) {
  return (
    <span
      className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-semibold ${clase}`}
    >
      {children}
    </span>
  );
}

function TarjetaKPI({
  titulo,
  valor,
  detalle,
  color = "text-white",
}: {
  titulo: string;
  valor: string | number;
  detalle: string;
  color?: string;
}) {
  return (
    <article className={panel}>
      <p className="text-sm text-slate-400">{titulo}</p>
      <p className={`mt-3 text-3xl font-bold ${color}`}>{valor}</p>
      <p className="mt-2 text-xs text-slate-500">{detalle}</p>
    </article>
  );
}

function ObjetoMetricas({ datos }: { datos: unknown }) {
  if (datos === null || datos === undefined) {
    return (
      <p className="text-sm text-slate-500">
        Todavía no hay métricas disponibles.
      </p>
    );
  }

  if (typeof datos !== "object") {
    return (
      <p className="break-words text-sm text-slate-300">
        {String(datos)}
      </p>
    );
  }

  if (Array.isArray(datos)) {
    if (datos.length === 0) {
      return (
        <p className="text-sm text-slate-500">
          No hay registros para mostrar.
        </p>
      );
    }

    return (
      <div className="space-y-3">
        {datos.map((item, index) => (
          <div
            key={index}
            className="rounded-xl border border-slate-800 bg-[#0b1425] p-3"
          >
            <p className="mb-2 text-xs text-slate-500">
              Registro {index + 1}
            </p>
            <ObjetoMetricas datos={item} />
          </div>
        ))}
      </div>
    );
  }

  const entradas = Object.entries(
    datos as Record<string, unknown>
  ).filter(([clave]) => clave !== "_id");

  if (entradas.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        No hay datos para mostrar.
      </p>
    );
  }

  return (
    <div className="space-y-2">
      {entradas.map(([clave, valor]) => (
        <div
          key={clave}
          className="flex flex-col justify-between gap-1 border-b border-slate-800/80 py-2 sm:flex-row sm:gap-4"
        >
          <span className="break-words text-sm text-slate-400">
            {clave.replaceAll("_", " ")}
          </span>
          <div className="min-w-0 text-sm text-slate-200 sm:max-w-[65%]">
            {valor !== null && typeof valor === "object" ? (
              <ObjetoMetricas datos={valor} />
            ) : (
              <span className="break-words">
                {valor === null || valor === undefined
                  ? "—"
                  : String(valor)}
              </span>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function Home() {
  const [seccion, setSeccion] = useState("Resumen");
  const [envios, setEnvios] = useState<Envio[]>([]);
  const [alertas, setAlertas] = useState<AlertaSLA[]>([]);
  const [metricas, setMetricas] = useState<Metricas | null>(null);
  const [busqueda, setBusqueda] = useState("");
  const [estadoFiltro, setEstadoFiltro] = useState("Todos");
  const [soloAbiertas, setSoloAbiertas] = useState(false);
  const [envioSeleccionado, setEnvioSeleccionado] = useState("");
  const [cargando, setCargando] = useState(true);
  const [error, setError] = useState("");
  const [horaActualizacion, setHoraActualizacion] = useState("--:--:--");

  const cargarDatos = useCallback(async (mostrarCarga = false) => {
    if (mostrarCarga) setCargando(true);

    try {
      const [
        respuestaMetricas,
        respuestaEnvios,
        respuestaAlertas,
      ] = await Promise.all([
        fetch("/api/metricas", { cache: "no-store" }),
        fetch("/api/envios", { cache: "no-store" }),
        fetch("/api/alertas", { cache: "no-store" }),
      ]);

      if (!respuestaMetricas.ok) {
        throw new Error("No se pudieron cargar las métricas.");
      }

      if (!respuestaEnvios.ok) {
        throw new Error("No se pudieron cargar los envíos.");
      }

      if (!respuestaAlertas.ok) {
        throw new Error("No se pudieron cargar las alertas SLA.");
      }

      const datosMetricas: Metricas =
        await respuestaMetricas.json();
      const datosEnvios: Envio[] =
        await respuestaEnvios.json();
      const datosAlertas: AlertaSLA[] =
        await respuestaAlertas.json();

      setMetricas(datosMetricas);
      setEnvios(Array.isArray(datosEnvios) ? datosEnvios : []);
      setAlertas(Array.isArray(datosAlertas) ? datosAlertas : []);
      setError("");
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Ocurrió un error al cargar los datos."
      );
    } finally {
      setCargando(false);
    }
  }, []);

  useEffect(() => {
    const actualizarHora = () => {
      setHoraActualizacion(
        new Date().toLocaleTimeString("es-AR")
      );
    };

    actualizarHora();
    void cargarDatos(true);

    const intervalo = window.setInterval(() => {
      actualizarHora();
      void cargarDatos();
    }, 10000);

    return () => window.clearInterval(intervalo);
  }, [cargarDatos]);

  const enviosFiltrados = useMemo(() => {
    const termino = normalizar(busqueda.trim());

    return envios.filter((envio) => {
      const coincideBusqueda =
        !termino ||
        normalizar(envio.shipmentId || "").includes(termino) ||
        normalizar(envio.estadoActual || "").includes(termino) ||
        normalizar(envio.zonaId || "").includes(termino) ||
        normalizar(envio.hubId || "").includes(termino);

      const coincideEstado =
        estadoFiltro === "Todos" ||
        normalizar(envio.estadoActual || "") ===
        normalizar(estadoFiltro);

      return coincideBusqueda && coincideEstado;
    });
  }, [envios, busqueda, estadoFiltro]);

  const envioDetalle = useMemo(() => {
    return (
      envios.find(
        (envio) => envio.shipmentId === envioSeleccionado
      ) ||
      enviosFiltrados[0] ||
      null
    );
  }, [envios, enviosFiltrados, envioSeleccionado]);

  const alertasAbiertas = useMemo(
    () =>
      alertas.filter(
        (alerta) =>
          normalizar(alerta.estado || "") === "abierta"
      ),
    [alertas]
  );

  const alertasCriticas = alertasAbiertas.filter(
    (alerta) =>
      normalizar(alerta.prioridad || "") === "critica"
  );

  const alertasFiltradas = useMemo(() => {
    const termino = normalizar(busqueda.trim());

    return alertas.filter((alerta) => {
      const coincideBusqueda =
        !termino ||
        normalizar(alerta.shipmentId || "").includes(termino) ||
        normalizar(alerta.regla || "").includes(termino) ||
        normalizar(alerta.descripcion || "").includes(termino) ||
        normalizar(alerta.zonaId || "").includes(termino);

      const coincideAbiertas =
        !soloAbiertas ||
        normalizar(alerta.estado || "") === "abierta";

      return coincideBusqueda && coincideAbiertas;
    });
  }, [alertas, busqueda, soloAbiertas]);

  const fallidos = envios.filter((envio) =>
    normalizar(envio.estadoActual || "").includes("fallid")
  ).length;

  const entregados = envios.filter((envio) =>
    normalizar(envio.estadoActual || "").includes("entregado")
  ).length;

  const enCurso = envios.length - entregados;

  const ultimaActualizacion = useMemo(() => {
    const fechas = envios
      .map((envio) => envio.actualizadoEn)
      .filter(Boolean)
      .map((valor) => new Date(valor).getTime())
      .filter((valor) => !Number.isNaN(valor));

    if (fechas.length === 0) return "Sin información";

    return fecha(new Date(Math.max(...fechas)).toISOString());
  }, [envios]);

  const estadosDisponibles = useMemo(
    () =>
      Array.from(
        new Set(
          envios
            .map((envio) => envio.estadoActual)
            .filter((estado): estado is string => Boolean(estado))
        )
      ).sort(),
    [envios]
  );

  function abrirSeguimiento(shipmentId: string) {
    setEnvioSeleccionado(shipmentId);
    setBusqueda(shipmentId);
    setSeccion("Seguimiento");
  }

  return (
    <main className="min-h-screen bg-[#080f1d] text-slate-100">
      <div className="flex min-h-screen">
        <aside className="hidden w-64 shrink-0 border-r border-slate-800 bg-[#0b1425] p-5 lg:flex lg:flex-col">
          <div className="mb-10">
            <div className="flex items-center gap-3">
              <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-emerald-400 font-black text-slate-950">
                RS
              </div>
              <div>
                <h1 className="text-lg font-bold">RutaSur</h1>
                <p className="text-xs text-slate-500">
                  Plataforma logística
                </p>
              </div>
            </div>
          </div>

          <p className="mb-3 px-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
            Operaciones
          </p>

          <nav className="space-y-1">
            {MENU.map((item) => (
              <button
                key={item}
                onClick={() => {
                  setSeccion(item);
                  if (item !== "Alertas SLA") {
                    setSoloAbiertas(false);
                  }
                }}
                className={`w-full rounded-xl px-4 py-3 text-left text-sm transition ${seccion === item
                  ? "bg-emerald-400/10 font-semibold text-emerald-300"
                  : "text-slate-400 hover:bg-slate-800/70 hover:text-white"
                  }`}
              >
                {item === "Resumen" && "◈ "}
                {item === "Torre de control" && "▦ "}
                {item === "Alertas SLA" && "⚑ "}
                {item === "Seguimiento" && "⌕ "}
                {item === "Analítica" && "▥ "}
                {item === "Demostración" && "▷ "}
                {item}

                {item === "Alertas SLA" &&
                  alertasAbiertas.length > 0 && (
                    <span className="float-right rounded-full bg-red-400/15 px-2 py-0.5 text-xs text-red-300">
                      {alertasAbiertas.length}
                    </span>
                  )}
              </button>
            ))}
          </nav>

          <div className="mt-auto rounded-2xl border border-slate-800 bg-[#111c30] p-4">
            <div className="flex items-center gap-2">
              <span className="h-2 w-2 rounded-full bg-emerald-400" />
              <p className="text-sm font-semibold">Panel conectado</p>
            </div>
            <p className="mt-2 text-xs leading-5 text-slate-500">
              Datos consultados desde MongoDB. Actualización automática
              cada 10 segundos.
            </p>
          </div>
        </aside>

        <div className="min-w-0 flex-1">
          <header className="sticky top-0 z-10 border-b border-slate-800 bg-[#080f1d]/95 px-5 py-4 backdrop-blur-xl md:px-8">
            <div className="mx-auto flex max-w-[1600px] flex-col justify-between gap-4 sm:flex-row sm:items-center">
              <div>
                <p className="text-xs font-medium uppercase tracking-[0.18em] text-emerald-400">
                  RutaSur / Operaciones
                </p>
                <h2 className="mt-1 text-xl font-bold">{seccion}</h2>
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <span className="flex items-center gap-2 text-xs text-slate-400">
                  <span className="h-2 w-2 rounded-full bg-emerald-400" />
                  Actualización automática
                </span>
                <button
                  onClick={() => void cargarDatos(true)}
                  className="rounded-xl border border-slate-700 px-4 py-2 text-sm font-medium transition hover:border-emerald-400 hover:text-emerald-300"
                >
                  ↻ Actualizar
                </button>
              </div>
            </div>

            <nav className="mx-auto mt-4 flex max-w-[1600px] gap-2 overflow-x-auto pb-1 lg:hidden">
              {MENU.map((item) => (
                <button
                  key={item}
                  onClick={() => setSeccion(item)}
                  className={`shrink-0 rounded-xl px-3 py-2 text-xs ${seccion === item
                    ? "bg-emerald-400/10 text-emerald-300"
                    : "bg-slate-800 text-slate-400"
                    }`}
                >
                  {item}
                  {item === "Alertas SLA" &&
                    alertasAbiertas.length > 0
                    ? ` (${alertasAbiertas.length})`
                    : ""}
                </button>
              ))}
            </nav>
          </header>

          <div className="mx-auto max-w-[1600px] space-y-6 p-5 md:p-8">
            {error && (
              <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-red-400/30 bg-red-400/10 p-4 text-sm text-red-200">
                <p>{error}</p>
                <button
                  onClick={() => void cargarDatos(true)}
                  className="rounded-lg border border-red-300/30 px-3 py-2"
                >
                  Reintentar
                </button>
              </div>
            )}

            {cargando && (
              <p className="text-sm text-slate-400">
                Cargando información de RutaSur...
              </p>
            )}

            {seccion === "Resumen" && (
              <section className="space-y-6">
                <div className="flex flex-col justify-between gap-3 md:flex-row md:items-end">
                  <div>
                    <p className="text-sm font-medium text-emerald-400">
                      VISTA GENERAL
                    </p>
                    <h3 className="mt-1 text-2xl font-bold">
                      Centro de operaciones
                    </h3>
                    <p className="mt-2 text-sm text-slate-400">
                      Estado general de los envíos, las promesas y las alertas.
                    </p>
                  </div>
                  <Badge clase="bg-emerald-400/10 text-emerald-300">
                    MongoDB conectado por API
                  </Badge>
                </div>

                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <TarjetaKPI
                    titulo="Envíos registrados"
                    valor={envios.length}
                    detalle="Envíos cargados desde MongoDB"
                  />
                  <TarjetaKPI
                    titulo="En proceso"
                    valor={enCurso}
                    detalle="No figuran como entregados"
                    color="text-sky-300"
                  />
                  <TarjetaKPI
                    titulo="Entregados"
                    valor={entregados}
                    detalle="Estado actual del envío"
                    color="text-emerald-300"
                  />
                  <TarjetaKPI
                    titulo="Alertas SLA abiertas"
                    valor={alertasAbiertas.length}
                    detalle={`${alertasCriticas.length} de prioridad crítica`}
                    color={
                      alertasAbiertas.length > 0
                        ? "text-amber-300"
                        : "text-emerald-300"
                    }
                  />
                </div>

                <div className="grid gap-6 xl:grid-cols-3">
                  <div className={`${panel} xl:col-span-2`}>
                    <div className="mb-5 flex items-center justify-between gap-3">
                      <div>
                        <h3 className="font-semibold">Envíos recientes</h3>
                        <p className="mt-1 text-xs text-slate-500">
                          Últimos registros recibidos
                        </p>
                      </div>
                      <button
                        onClick={() => setSeccion("Torre de control")}
                        className="text-sm text-emerald-300 hover:text-emerald-200"
                      >
                        Ver todos →
                      </button>
                    </div>

                    {envios.slice(0, 6).length === 0 ? (
                      <p className="py-8 text-center text-sm text-slate-500">
                        No hay envíos registrados.
                      </p>
                    ) : (
                      <div className="space-y-3">
                        {envios.slice(0, 6).map((envio) => (
                          <button
                            key={envio.shipmentId}
                            onClick={() =>
                              abrirSeguimiento(envio.shipmentId)
                            }
                            className="flex w-full flex-col justify-between gap-3 rounded-xl border border-slate-800 bg-[#0b1425] p-4 text-left transition hover:border-slate-600 sm:flex-row sm:items-center"
                          >
                            <div>
                              <p className="break-all text-sm font-semibold">
                                {envio.shipmentId}
                              </p>
                              <p className="mt-1 text-xs text-slate-500">
                                {envio.zonaId || "Sin zona"} ·{" "}
                                {envio.hubId || "Sin hub"}
                              </p>
                            </div>
                            <div className="flex items-center gap-3">
                              <Badge>
                                {etiquetaEstado(envio.estadoActual)}
                              </Badge>
                              <span className="text-xs text-slate-500">
                                Ver →
                              </span>
                            </div>
                          </button>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className={panel}>
                    <h3 className="font-semibold">Estado del sistema</h3>
                    <p className="mt-1 text-xs text-slate-500">
                      Indicadores de los datos disponibles
                    </p>

                    <div className="mt-5 space-y-4">
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-sm text-slate-400">Envíos</span>
                        <Badge clase="bg-emerald-400/10 text-emerald-300">
                          {envios.length} registros
                        </Badge>
                      </div>
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-sm text-slate-400">
                          Alertas abiertas
                        </span>
                        <Badge
                          clase={
                            alertasAbiertas.length
                              ? "bg-amber-400/10 text-amber-300"
                              : "bg-emerald-400/10 text-emerald-300"
                          }
                        >
                          {alertasAbiertas.length}
                        </Badge>
                      </div>
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-sm text-slate-400">
                          Alertas críticas
                        </span>
                        <Badge
                          clase={
                            alertasCriticas.length
                              ? "bg-red-400/10 text-red-300"
                              : "bg-emerald-400/10 text-emerald-300"
                          }
                        >
                          {alertasCriticas.length}
                        </Badge>
                      </div>
                      <div className="border-t border-slate-800 pt-4">
                        <p className="text-xs text-slate-500">
                          Última actualización de un envío
                        </p>
                        <p className="mt-2 text-sm text-slate-300">
                          {ultimaActualizacion}
                        </p>
                      </div>
                    </div>

                    <button
                      onClick={() => setSeccion("Alertas SLA")}
                      className="mt-6 w-full rounded-xl bg-emerald-400 px-4 py-3 text-sm font-bold text-slate-950 transition hover:bg-emerald-300"
                    >
                      Consultar alertas SLA
                    </button>
                  </div>
                </div>

                <div className={panel}>
                  <h3 className="font-semibold">Actividad de envíos</h3>
                  <p className="mt-1 text-xs text-slate-500">
                    Distribución según el estado actual registrado
                  </p>

                  <div className="mt-5 space-y-4">
                    {[
                      {
                        nombre: "Entregados",
                        cantidad: entregados,
                        color: "bg-emerald-400",
                      },
                      {
                        nombre: "Con fallos",
                        cantidad: fallidos,
                        color: "bg-red-400",
                      },
                      {
                        nombre: "En proceso",
                        cantidad: enCurso,
                        color: "bg-sky-400",
                      },
                    ].map((item) => (
                      <div key={item.nombre}>
                        <div className="mb-2 flex justify-between text-sm">
                          <span className="text-slate-400">
                            {item.nombre}
                          </span>
                          <span className="font-semibold">
                            {item.cantidad}
                          </span>
                        </div>
                        <div className="h-2 overflow-hidden rounded-full bg-slate-800">
                          <div
                            className={`h-full rounded-full ${item.color}`}
                            style={{
                              width: `${envios.length
                                ? (item.cantidad / envios.length) * 100
                                : 0
                                }%`,
                            }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </section>
            )}

            {seccion === "Torre de control" && (
              <section className="space-y-6">
                <div>
                  <p className="text-sm font-medium text-emerald-400">
                    VISIBILIDAD OPERATIVA
                  </p>
                  <h3 className="mt-1 text-2xl font-bold">
                    Torre de control
                  </h3>
                  <p className="mt-2 text-sm text-slate-400">
                    Buscá envíos, consultá estados y abrí su seguimiento.
                  </p>
                </div>

                <div className="grid gap-4 md:grid-cols-[1fr_240px]">
                  <input
                    className={inputStyle}
                    value={busqueda}
                    onChange={(e) => setBusqueda(e.target.value)}
                    placeholder="Buscar por envío, estado, zona o hub..."
                  />
                  <select
                    className={inputStyle}
                    value={estadoFiltro}
                    onChange={(e) => setEstadoFiltro(e.target.value)}
                  >
                    <option value="Todos">Todos los estados</option>
                    {estadosDisponibles.map((estado) => (
                      <option key={estado} value={estado}>
                        {etiquetaEstado(estado)}
                      </option>
                    ))}
                  </select>
                </div>

                <div className={panel}>
                  <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                    <h3 className="font-semibold">Listado de envíos</h3>
                    <Badge>{enviosFiltrados.length} resultados</Badge>
                  </div>

                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[850px] text-left text-sm">
                      <thead>
                        <tr className="border-b border-slate-800 text-xs uppercase text-slate-500">
                          <th className="px-3 py-3 font-medium">Envío</th>
                          <th className="px-3 py-3 font-medium">Estado</th>
                          <th className="px-3 py-3 font-medium">Hub</th>
                          <th className="px-3 py-3 font-medium">Zona</th>
                          <th className="px-3 py-3 font-medium">Intentos</th>
                          <th className="px-3 py-3 font-medium">Actualizado</th>
                          <th className="px-3 py-3 font-medium">Acción</th>
                        </tr>
                      </thead>
                      <tbody>
                        {enviosFiltrados.map((envio) => (
                          <tr
                            key={envio.shipmentId}
                            className="border-b border-slate-800/70 last:border-0"
                          >
                            <td className="px-3 py-4 font-medium">
                              <span className="break-all">
                                {envio.shipmentId}
                              </span>
                            </td>
                            <td className="px-3 py-4">
                              <Badge>
                                {etiquetaEstado(envio.estadoActual)}
                              </Badge>
                            </td>
                            <td className="px-3 py-4 text-slate-400">
                              {envio.hubId || "—"}
                            </td>
                            <td className="px-3 py-4 text-slate-400">
                              {envio.zonaId || "—"}
                            </td>
                            <td className="px-3 py-4">
                              {envio.intentos ?? 0}
                            </td>
                            <td className="px-3 py-4 text-xs text-slate-400">
                              {fecha(envio.actualizadoEn)}
                            </td>
                            <td className="px-3 py-4">
                              <button
                                onClick={() =>
                                  abrirSeguimiento(envio.shipmentId)
                                }
                                className="text-emerald-300 hover:text-emerald-200"
                              >
                                Ver →
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {enviosFiltrados.length === 0 && (
                    <p className="py-8 text-center text-sm text-slate-500">
                      No se encontraron envíos con esos filtros.
                    </p>
                  )}
                </div>
              </section>
            )}

            {seccion === "Alertas SLA" && (
              <section className="space-y-6">
                <div className="flex flex-col justify-between gap-3 md:flex-row md:items-end">
                  <div>
                    <p className="text-sm font-medium text-emerald-400">
                      MONITOREO DE PROMESAS
                    </p>
                    <h3 className="mt-1 text-2xl font-bold">
                      Alertas de nivel de servicio
                    </h3>
                    <p className="mt-2 text-sm text-slate-400">
                      Alertas leídas desde MongoDB y actualizadas automáticamente.
                    </p>
                  </div>
                  <Badge clase="bg-emerald-400/10 text-emerald-300">
                    Monitor SLA
                  </Badge>
                </div>

                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <TarjetaKPI
                    titulo="Alertas registradas"
                    valor={alertas.length}
                    detalle="Total de alertas recuperadas"
                  />
                  <TarjetaKPI
                    titulo="Abiertas"
                    valor={alertasAbiertas.length}
                    detalle="Requieren seguimiento"
                    color="text-amber-300"
                  />
                  <TarjetaKPI
                    titulo="Prioridad crítica"
                    valor={alertasCriticas.length}
                    detalle="Entre las alertas abiertas"
                    color="text-red-300"
                  />
                  <TarjetaKPI
                    titulo="Cerradas"
                    valor={
                      alertas.filter(
                        (a) => normalizar(a.estado || "") === "cerrada"
                      ).length
                    }
                    detalle="Ya no se encuentran abiertas"
                    color="text-emerald-300"
                  />
                </div>

                <div className="grid gap-3 md:grid-cols-[1fr_auto]">
                  <input
                    className={inputStyle}
                    value={busqueda}
                    onChange={(e) => setBusqueda(e.target.value)}
                    placeholder="Buscar por envío, regla, descripción o zona..."
                  />

                  <button
                    onClick={() => setSoloAbiertas((actual) => !actual)}
                    className={`rounded-xl border px-4 py-3 text-sm font-medium transition ${soloAbiertas
                      ? "border-amber-400/40 bg-amber-400/10 text-amber-300"
                      : "border-slate-700 text-slate-300 hover:border-slate-500"
                      }`}
                  >
                    {soloAbiertas
                      ? "Mostrar todas"
                      : "Mostrar solo abiertas"}
                  </button>
                </div>

                <div className="flex items-center justify-between gap-3">
                  <h3 className="font-semibold">Registro de alertas</h3>
                  <Badge>{alertasFiltradas.length} resultados</Badge>
                </div>

                {alertasFiltradas.length === 0 ? (
                  <div className={`${panel} py-12 text-center`}>
                    <p className="font-semibold">
                      No hay alertas para mostrar
                    </p>
                    <p className="mt-2 text-sm text-slate-400">
                      Probá cambiar la búsqueda o mostrar todas las alertas.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {alertasFiltradas.map((alerta, index) => {
                      const abierta =
                        normalizar(alerta.estado || "") === "abierta";

                      return (
                        <article
                          key={
                            alerta.alertaId ||
                            `${alerta.shipmentId}-${alerta.regla}-${index}`
                          }
                          className={panel}
                        >
                          <div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-start">
                            <div className="min-w-0">
                              <p className="break-all text-base font-bold">
                                {alerta.shipmentId || "Envío sin identificador"}
                              </p>
                              <p className="mt-2 text-sm leading-6 text-slate-400">
                                {alerta.descripcion || "Sin descripción"}
                              </p>
                            </div>

                            <div className="flex shrink-0 flex-wrap gap-2">
                              <Badge
                                clase={prioridadClase(alerta.prioridad || "")}
                              >
                                {alerta.prioridad || "Sin prioridad"}
                              </Badge>
                              <Badge
                                clase={estadoClase(alerta.estado || "")}
                              >
                                {abierta ? "Abierta" : "Cerrada"}
                              </Badge>
                            </div>
                          </div>

                          <div className="mt-5 grid gap-4 border-t border-slate-800 pt-4 sm:grid-cols-2 xl:grid-cols-3">
                            <div>
                              <p className="text-xs text-slate-500">Regla SLA</p>
                              <p className="mt-1 break-words text-sm text-slate-200">
                                {alerta.regla || "—"}
                              </p>
                            </div>

                            <div>
                              <p className="text-xs text-slate-500">Hub / Zona</p>
                              <p className="mt-1 text-sm text-slate-200">
                                {alerta.hubId || "Sin hub"} /{" "}
                                {alerta.zonaId || "Sin zona"}
                              </p>
                            </div>

                            <div>
                              <p className="text-xs text-slate-500">
                                Fecha prometida
                              </p>
                              <p className="mt-1 text-sm text-slate-200">
                                {fecha(alerta.fechaPrometida)}
                              </p>
                            </div>

                            <div>
                              <p className="text-xs text-slate-500">Creada</p>
                              <p className="mt-1 text-sm text-slate-200">
                                {fecha(alerta.creadaEn)}
                              </p>
                            </div>

                            <div>
                              <p className="text-xs text-slate-500">
                                Última actualización
                              </p>
                              <p className="mt-1 text-sm text-slate-200">
                                {fecha(alerta.actualizadaEn)}
                              </p>
                            </div>

                            {!abierta && (
                              <div>
                                <p className="text-xs text-slate-500">
                                  Motivo de cierre
                                </p>
                                <p className="mt-1 break-words text-sm text-emerald-300">
                                  {alerta.motivoCierre ||
                                    "Sin motivo registrado"}
                                </p>
                                {alerta.cerradaEn && (
                                  <p className="mt-1 text-xs text-slate-500">
                                    Cerrada: {fecha(alerta.cerradaEn)}
                                  </p>
                                )}
                              </div>
                            )}
                          </div>

                          <div className="mt-4 flex justify-end border-t border-slate-800 pt-4">
                            <button
                              onClick={() =>
                                abrirSeguimiento(alerta.shipmentId)
                              }
                              className="text-sm font-medium text-emerald-300 hover:text-emerald-200"
                            >
                              Ver seguimiento del envío →
                            </button>
                          </div>
                        </article>
                      );
                    })}
                  </div>
                )}
              </section>
            )}

            {seccion === "Seguimiento" && (
              <section className="space-y-6">
                <div>
                  <p className="text-sm font-medium text-emerald-400">
                    TRAZABILIDAD
                  </p>
                  <h3 className="mt-1 text-2xl font-bold">
                    Seguimiento de envíos
                  </h3>
                  <p className="mt-2 text-sm text-slate-400">
                    Seleccioná un envío para consultar su historial de eventos.
                  </p>
                </div>

                <input
                  className={inputStyle}
                  value={busqueda}
                  onChange={(e) => setBusqueda(e.target.value)}
                  placeholder="Buscar envío por identificador, estado, zona o hub..."
                />

                <div className="grid gap-6 xl:grid-cols-[minmax(280px,0.85fr)_minmax(0,1.5fr)]">
                  <div className={`${panel} h-fit`}>
                    <div className="mb-4 flex items-center justify-between gap-3">
                      <h3 className="font-semibold">Envíos disponibles</h3>
                      <Badge>{enviosFiltrados.length}</Badge>
                    </div>

                    <div className="max-h-[650px] space-y-2 overflow-y-auto">
                      {enviosFiltrados.map((envio) => (
                        <button
                          key={envio.shipmentId}
                          onClick={() =>
                            setEnvioSeleccionado(envio.shipmentId)
                          }
                          className={`w-full rounded-xl border p-4 text-left transition ${envioDetalle?.shipmentId === envio.shipmentId
                            ? "border-emerald-400/40 bg-emerald-400/5"
                            : "border-slate-800 bg-[#0b1425] hover:border-slate-600"
                            }`}
                        >
                          <p className="break-all text-sm font-semibold">
                            {envio.shipmentId}
                          </p>
                          <div className="mt-2 flex flex-wrap items-center gap-2">
                            <Badge>
                              {etiquetaEstado(envio.estadoActual)}
                            </Badge>
                            <span className="text-xs text-slate-500">
                              {envio.zonaId || "Sin zona"}
                            </span>
                          </div>
                        </button>
                      ))}

                      {enviosFiltrados.length === 0 && (
                        <p className="py-6 text-center text-sm text-slate-500">
                          No se encontraron envíos.
                        </p>
                      )}
                    </div>
                  </div>

                  <div className="space-y-6">
                    {!envioDetalle ? (
                      <div className={`${panel} py-12 text-center`}>
                        <p className="font-semibold">Seleccioná un envío</p>
                        <p className="mt-2 text-sm text-slate-400">
                          El detalle y los eventos aparecerán aquí.
                        </p>
                      </div>
                    ) : (
                      <>
                        <div className={panel}>
                          <div className="flex flex-col justify-between gap-4 sm:flex-row">
                            <div className="min-w-0">
                              <p className="text-xs uppercase tracking-wider text-slate-500">
                                Identificador del envío
                              </p>
                              <h3 className="mt-2 break-all text-xl font-bold">
                                {envioDetalle.shipmentId}
                              </h3>
                            </div>
                            <Badge>
                              {etiquetaEstado(envioDetalle.estadoActual)}
                            </Badge>
                          </div>

                          <div className="mt-6 grid gap-4 sm:grid-cols-2">
                            {[
                              ["Hub", envioDetalle.hubId || "—"],
                              ["Zona", envioDetalle.zonaId || "—"],
                              [
                                "Repartidor",
                                envioDetalle.repartidorId || "Sin asignar",
                              ],
                              [
                                "Intentos",
                                String(envioDetalle.intentos ?? 0),
                              ],
                              ["Creado", fecha(envioDetalle.creadoEn)],
                              [
                                "Actualizado",
                                fecha(envioDetalle.actualizadoEn),
                              ],
                            ].map(([titulo, valor]) => (
                              <div
                                key={titulo}
                                className="rounded-xl bg-[#0b1425] p-4"
                              >
                                <p className="text-xs text-slate-500">
                                  {titulo}
                                </p>
                                <p className="mt-2 break-words text-sm font-medium text-slate-200">
                                  {valor}
                                </p>
                              </div>
                            ))}
                          </div>
                        </div>

                        <div className={panel}>
                          <div className="mb-6">
                            <h3 className="font-semibold">
                              Historial de eventos
                            </h3>
                            <p className="mt-1 text-xs text-slate-500">
                              Orden cronológico según la secuencia del envío
                            </p>
                          </div>

                          {(!envioDetalle.historial ||
                            envioDetalle.historial.length === 0) && (
                              <p className="py-8 text-center text-sm text-slate-500">
                                Este envío todavía no tiene eventos en el historial proyectado.
                              </p>
                            )}

                          <div className="space-y-0">
                            {[...(envioDetalle.historial || [])]
                              .sort(
                                (a, b) =>
                                  (a.secuencia ?? 0) -
                                  (b.secuencia ?? 0)
                              )
                              .map((evento, index, lista) => (
                                <div
                                  key={
                                    evento.eventId ||
                                    `${evento.secuencia}-${evento.tipo}-${index}`
                                  }
                                  className="relative flex gap-4 pb-7 last:pb-0"
                                >
                                  <div className="flex flex-col items-center">
                                    <div className="z-[1] flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-emerald-400/30 bg-emerald-400/10 text-sm text-emerald-300">
                                      {index + 1}
                                    </div>
                                    {index < lista.length - 1 && (
                                      <div className="absolute top-9 bottom-0 w-px bg-slate-800" />
                                    )}
                                  </div>

                                  <div className="min-w-0 flex-1 rounded-xl border border-slate-800 bg-[#0b1425] p-4">
                                    <div className="flex flex-wrap items-start justify-between gap-2">
                                      <p className="break-words font-semibold">
                                        {etiquetaEstado(evento.tipo)}
                                      </p>
                                      <Badge>
                                        Secuencia {evento.secuencia}
                                      </Badge>
                                    </div>
                                    <p className="mt-2 text-xs text-slate-400">
                                      {fecha(evento.ocurridoEn)}
                                    </p>
                                  </div>
                                </div>
                              ))}
                          </div>
                        </div>

                        <div className={panel}>
                          <h3 className="font-semibold">Alertas asociadas</h3>
                          <p className="mt-1 text-xs text-slate-500">
                            Alertas SLA relacionadas con este envío
                          </p>

                          <div className="mt-4 space-y-3">
                            {alertas
                              .filter(
                                (alerta) =>
                                  alerta.shipmentId ===
                                  envioDetalle.shipmentId
                              )
                              .map((alerta, index) => (
                                <div
                                  key={
                                    alerta.alertaId ||
                                    `${alerta.regla}-${index}`
                                  }
                                  className="rounded-xl border border-slate-800 bg-[#0b1425] p-4"
                                >
                                  <div className="flex flex-wrap justify-between gap-2">
                                    <p className="font-semibold">
                                      {alerta.regla}
                                    </p>
                                    <Badge
                                      clase={prioridadClase(
                                        alerta.prioridad || ""
                                      )}
                                    >
                                      {alerta.prioridad}
                                    </Badge>
                                  </div>
                                  <p className="mt-2 text-sm text-slate-400">
                                    {alerta.descripcion}
                                  </p>
                                  <div className="mt-3 flex flex-wrap gap-2">
                                    <Badge
                                      clase={estadoClase(alerta.estado || "")}
                                    >
                                      {alerta.estado}
                                    </Badge>
                                    {normalizar(alerta.estado || "") !==
                                      "abierta" && (
                                        <span className="text-xs text-slate-500">
                                          {alerta.motivoCierre || "Cerrada"}
                                        </span>
                                      )}
                                  </div>
                                </div>
                              ))}

                            {alertas.filter(
                              (alerta) =>
                                alerta.shipmentId ===
                                envioDetalle.shipmentId
                            ).length === 0 && (
                                <p className="py-4 text-sm text-slate-500">
                                  No hay alertas asociadas a este envío.
                                </p>
                              )}
                          </div>
                        </div>
                      </>
                    )}
                  </div>
                </div>
              </section>
            )}

            {seccion === "Analítica" && (
              <section className="space-y-6">
                <div>
                  <p className="text-sm font-medium text-emerald-400">
                    INTELIGENCIA OPERATIVA
                  </p>
                  <h3 className="mt-1 text-2xl font-bold">
                    Analítica de envíos
                  </h3>
                  <p className="mt-2 text-sm text-slate-400">
                    Métricas calculadas por el servicio de analítica y almacenadas en MongoDB.
                  </p>
                </div>

                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <TarjetaKPI
                    titulo="Envíos registrados"
                    valor={envios.length}
                    detalle="Vista actual de envíos"
                  />
                  <TarjetaKPI
                    titulo="Entregados"
                    valor={entregados}
                    detalle="Según el estado actual"
                    color="text-emerald-300"
                  />
                  <TarjetaKPI
                    titulo="Con fallos"
                    valor={fallidos}
                    detalle="Estado actual con fallo"
                    color="text-red-300"
                  />
                  <TarjetaKPI
                    titulo="Alertas SLA abiertas"
                    valor={alertasAbiertas.length}
                    detalle="Alertas pendientes de resolución"
                    color="text-amber-300"
                  />
                </div>

                <div className={panel}>
                  <h3 className="font-semibold">
                    Métricas calculadas por el servicio
                  </h3>
                  <p className="mt-1 text-xs text-slate-500">
                    Se muestran los campos que devuelve la API de métricas.
                  </p>
                  <div className="mt-5">
                    <ObjetoMetricas datos={metricas} />
                  </div>
                </div>

                <div className={panel}>
                  <h3 className="font-semibold">
                    Fallos registrados en los envíos
                  </h3>
                  <p className="mt-1 text-xs text-slate-500">
                    Conteo basado en los estados actuales de los envíos.
                  </p>

                  <div className="mt-5 space-y-4">
                    {[
                      {
                        titulo: "Entregados",
                        valor: entregados,
                        color: "bg-emerald-400",
                      },
                      {
                        titulo: "Estados con fallo",
                        valor: fallidos,
                        color: "bg-red-400",
                      },
                      {
                        titulo: "En proceso",
                        valor: enCurso,
                        color: "bg-sky-400",
                      },
                    ].map((dato) => (
                      <div key={dato.titulo}>
                        <div className="mb-2 flex justify-between gap-3 text-sm">
                          <span className="text-slate-400">
                            {dato.titulo}
                          </span>
                          <span>{dato.valor}</span>
                        </div>
                        <div className="h-2 rounded-full bg-slate-800">
                          <div
                            className={`h-2 rounded-full ${dato.color}`}
                            style={{
                              width: `${envios.length
                                ? (dato.valor / envios.length) * 100
                                : 0
                                }%`,
                            }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </section>
            )}

            {seccion === "Demostración" && (
              <section className="space-y-6">
                <div>
                  <p className="text-sm font-medium text-emerald-400">
                    ENTORNO DE PRUEBA
                  </p>
                  <h3 className="mt-1 text-2xl font-bold">
                    Demostración del sistema
                  </h3>
                  <p className="mt-2 text-sm text-slate-400">
                    Panel para verificar el estado de los datos y preparar los escenarios de prueba.
                  </p>
                </div>

                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <TarjetaKPI
                    titulo="Envíos visibles"
                    valor={envios.length}
                    detalle="Documentos devueltos por la API"
                  />
                  <TarjetaKPI
                    titulo="Alertas registradas"
                    valor={alertas.length}
                    detalle="Abiertas y cerradas"
                  />
                  <TarjetaKPI
                    titulo="Alertas abiertas"
                    valor={alertasAbiertas.length}
                    detalle="Estado ABIERTA"
                    color="text-amber-300"
                  />
                  <TarjetaKPI
                    titulo="Alertas críticas abiertas"
                    valor={alertasCriticas.length}
                    detalle="Prioridad crítica"
                    color="text-red-300"
                  />
                </div>

                <div className={panel}>
                  <h3 className="font-semibold">Comprobaciones rápidas</h3>
                  <p className="mt-1 text-xs text-slate-500">
                    Comprobaciones de lectura desde la interfaz.
                  </p>

                  <div className="mt-5 space-y-3">
                    {[
                      {
                        titulo: "API de envíos",
                        ok: !cargando && !error,
                        detalle: `${envios.length} envíos cargados`,
                      },
                      {
                        titulo: "API de alertas SLA",
                        ok: !cargando && !error,
                        detalle: `${alertas.length} alertas cargadas`,
                      },
                      {
                        titulo: "API de métricas",
                        ok:
                          !cargando &&
                          !error &&
                          metricas !== null,
                        detalle:
                          metricas !== null
                            ? "Se recibieron datos"
                            : "Sin métricas disponibles",
                      },
                    ].map((prueba) => (
                      <div
                        key={prueba.titulo}
                        className="flex flex-col justify-between gap-3 rounded-xl border border-slate-800 bg-[#0b1425] p-4 sm:flex-row sm:items-center"
                      >
                        <div>
                          <p className="text-sm font-semibold">
                            {prueba.titulo}
                          </p>
                          <p className="mt-1 text-xs text-slate-500">
                            {prueba.detalle}
                          </p>
                        </div>
                        <Badge
                          clase={
                            prueba.ok
                              ? "bg-emerald-400/10 text-emerald-300"
                              : "bg-amber-400/10 text-amber-300"
                          }
                        >
                          {prueba.ok ? "Datos recibidos" : "Verificar"}
                        </Badge>
                      </div>
                    ))}
                  </div>
                </div>

                <div className={panel}>
                  <h3 className="font-semibold">Ir a una pantalla</h3>
                  <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    {MENU.filter(
                      (item) => item !== "Demostración"
                    ).map((item) => (
                      <button
                        key={item}
                        onClick={() => setSeccion(item)}
                        className="rounded-xl border border-slate-800 bg-[#0b1425] p-4 text-left transition hover:border-emerald-400/40"
                      >
                        <p className="font-semibold">{item}</p>
                        <p className="mt-2 text-xs text-slate-500">
                          Abrir sección →
                        </p>
                      </button>
                    ))}
                  </div>
                </div>
              </section>
            )}

            <footer className="flex flex-col justify-between gap-2 border-t border-slate-800 pt-5 text-xs text-slate-500 sm:flex-row">
              <p>RutaSur · Plataforma de seguimiento logístico</p>
              <p>Hora actual: {horaActualizacion}</p>
            </footer>
          </div>
        </div>
      </div>
    </main>
  );
}
