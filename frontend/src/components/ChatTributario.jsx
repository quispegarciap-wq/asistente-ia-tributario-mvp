import React, { useState, useRef, useEffect } from 'react';

// Endpoint con streaming (Server-Sent Events) del backend en Northflank.
const API_STREAM_URL =
  'https://p01--asistente-ia-tributario--qw7xms7w9jfx.code.run/api/chat/stream';

const ACCESOS_RAPIDOS = [
  {
    titulo: '¿Cuáles son los requisitos de una factura electrónica?',
    detalle: 'Datos obligatorios y condicionales del comprobante',
  },
  {
    titulo: '¿Qué es la afectación al IGV por ítem?',
    detalle: 'Cómo se declara en el detalle del comprobante',
  },
  {
    titulo: 'Explícame el catálogo de comprobantes de pago',
    detalle: 'Boleta, factura, nota de crédito y sus códigos',
  },
];

/* ---------- Render de texto: ## títulos, **negrita**, listas ---------- */
function conNegritas(texto) {
  return texto.split(/(\*\*[^*]+\*\*)/g).map((parte, i) =>
    parte.startsWith('**') && parte.endsWith('**') && parte.length > 4 ? (
      <strong key={i} className="font-semibold text-[#5fd3a5]">
        {parte.slice(2, -2)}
      </strong>
    ) : (
      <React.Fragment key={i}>{parte}</React.Fragment>
    )
  );
}

function renderMarkdown(texto) {
  const bloques = [];
  let lista = null;
  const cerrarLista = () => {
    if (lista) {
      bloques.push(lista);
      lista = null;
    }
  };

  texto.split('\n').forEach((raw) => {
    const linea = raw.trimEnd();
    const h = linea.match(/^\s*#{1,4}\s+(.*)$/);
    const ol = linea.match(/^\s*(\d+)[.)]\s+(.*)$/);
    const ul = linea.match(/^\s*[-*•]\s+(.*)$/);

    if (h) {
      cerrarLista();
      bloques.push({ tipo: 'h', texto: h[1] });
    } else if (ol) {
      if (!lista || lista.tipo !== 'ol') {
        cerrarLista();
        lista = { tipo: 'ol', inicio: parseInt(ol[1], 10), items: [] };
      }
      lista.items.push(ol[2]);
    } else if (ul) {
      if (!lista || lista.tipo !== 'ul') {
        cerrarLista();
        lista = { tipo: 'ul', items: [] };
      }
      lista.items.push(ul[1]);
    } else if (!linea.trim()) {
      cerrarLista();
    } else {
      cerrarLista();
      bloques.push({ tipo: 'p', texto: linea.trim() });
    }
  });
  cerrarLista();

  return bloques.map((b, i) => {
    if (b.tipo === 'h')
      return (
        <h3 key={i} className="mb-1 mt-4 text-base font-semibold text-white first:mt-0">
          {conNegritas(b.texto)}
        </h3>
      );
    if (b.tipo === 'ol')
      return (
        <ol key={i} start={b.inicio} className="my-2 list-decimal space-y-1 pl-5">
          {b.items.map((t, j) => (
            <li key={j}>{conNegritas(t)}</li>
          ))}
        </ol>
      );
    if (b.tipo === 'ul')
      return (
        <ul key={i} className="my-2 list-disc space-y-1 pl-5">
          {b.items.map((t, j) => (
            <li key={j}>{conNegritas(t)}</li>
          ))}
        </ul>
      );
    return (
      <p key={i} className="my-2 first:mt-0">
        {conNegritas(b.texto)}
      </p>
    );
  });
}

/* ------------------------------ Componente ------------------------------ */
export default function ChatTributario() {
  const [pregunta, setPregunta] = useState('');
  const [chatLog, setChatLog] = useState([]);
  const [loading, setLoading] = useState(false);
  const finRef = useRef(null);
  const abortRef = useRef(null);

  useEffect(() => {
    finRef.current?.scrollIntoView({ behavior: 'auto', block: 'end' });
  }, [chatLog]);

  const nuevaConversacion = () => {
    abortRef.current?.abort();
    setChatLog([]);
    setPregunta('');
    setLoading(false);
  };

  const enviar = async (texto) => {
    const q = texto.trim();
    if (!q || loading) return;

    const idIA = Date.now() + 1;
    setPregunta('');
    setLoading(true);
    setChatLog((prev) => [
      ...prev,
      { id: idIA - 1, rol: 'usuario', texto: q },
      { id: idIA, rol: 'ia', texto: '', estado: 'Enviando tu consulta...' },
    ]);

    const actualizar = (cambios) =>
      setChatLog((prev) =>
        prev.map((m) =>
          m.id === idIA ? { ...m, ...(typeof cambios === 'function' ? cambios(m) : cambios) } : m
        )
      );

    // Procesa un evento SSE: data: {"tipo": "...", "data": ...}
    const procesarEvento = (bloque) => {
      const linea = bloque.split('\n').find((l) => l.startsWith('data:'));
      if (!linea) return;
      let evento;
      try {
        evento = JSON.parse(linea.slice(5).trim());
      } catch {
        return;
      }
      const { tipo, data } = evento;

      if (tipo === 'estado') {
        actualizar({ estado: data });
      } else if (tipo === 'texto') {
        actualizar((m) => ({ texto: m.texto + data }));
      } else if (tipo === 'fin') {
        actualizar({
          fuentes: data.fuentes || [],
          sugerencias: data.sugerencias || [],
          degradado: !!data.degradado,
        });
      } else if (tipo === 'error') {
        actualizar({ error: data?.mensaje || 'Ocurrió un error al procesar la consulta.' });
      }
    };

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const response = await fetch(API_STREAM_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pregunta: q }),
        signal: controller.signal,
      });

      if (!response.ok || !response.body) {
        throw new Error(`HTTP ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
        const eventos = buffer.split('\n\n');
        buffer = eventos.pop();
        eventos.forEach(procesarEvento);
      }
      if (buffer.trim()) procesarEvento(buffer);
    } catch (error) {
      if (error.name !== 'AbortError') {
        actualizar({ error: 'No se pudo conectar con el servidor. Intenta de nuevo en unos segundos.' });
      }
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    enviar(pregunta);
  };

  const historial = chatLog.filter((m) => m.rol === 'usuario');
  const ultimo = chatLog[chatLog.length - 1];

  return (
    <div className="flex h-screen bg-[#131517] font-sans text-[#e6e8ea]">
      {/* Sidebar */}
      <aside className="hidden w-64 shrink-0 flex-col border-r border-[#26292d] bg-[#17191c] md:flex">
        <div className="flex items-center gap-3 border-b border-[#26292d] px-4 py-4">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-[#3fbf8f] text-sm font-bold text-[#0d2a1e]">
            AT
          </div>
          <span className="font-semibold">Asistente Tributario</span>
        </div>

        <div className="p-3">
          <button
            onClick={nuevaConversacion}
            className="w-full rounded-lg bg-[#1e2124] px-3 py-2 text-left text-sm font-medium transition-colors hover:bg-[#26292d] focus-visible:outline-2 focus-visible:outline-[#3fbf8f]"
          >
            Nueva conversación
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-2">
          <p className="mb-2 text-xs text-[#9ba1a6]">Historial de esta sesión</p>
          {historial.length === 0 ? (
            <p className="text-sm text-[#9ba1a6]">Tus preguntas van a aparecer aquí.</p>
          ) : (
            <ul className="space-y-1">
              {historial.map((m) => (
                <li key={m.id} className="truncate text-sm text-[#e6e8ea]/90" title={m.texto}>
                  {m.texto}
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="border-t border-[#26292d] px-4 py-3 text-xs text-[#9ba1a6]">
          Las respuestas se basan en la normativa cargada. Verifica siempre la fuente.
        </div>
      </aside>

      {/* Zona principal */}
      <main className="flex min-w-0 flex-1 flex-col">
        <div className="flex-1 overflow-y-auto px-4">
          <div className="mx-auto w-full max-w-3xl space-y-4 py-6">
            {chatLog.length === 0 ? (
              <div className="pt-10">
                <h2 className="text-3xl font-semibold">Hola, ¿en qué te ayudo hoy?</h2>
                <p className="mt-2 text-[#9ba1a6]">
                  Pregunta lo que necesites sobre normativa tributaria peruana, o elige un acceso rápido.
                </p>
                <div className="mt-8 space-y-3">
                  {ACCESOS_RAPIDOS.map((a) => (
                    <button
                      key={a.titulo}
                      onClick={() => enviar(a.titulo)}
                      className="block w-full rounded-xl border border-[#26292d] bg-[#1e2124] px-4 py-3 text-left transition-colors hover:border-[#3fbf8f] focus-visible:outline-2 focus-visible:outline-[#3fbf8f]"
                    >
                      <span className="block font-medium">{a.titulo}</span>
                      <span className="block text-sm text-[#9ba1a6]">{a.detalle}</span>
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              chatLog.map((msg) =>
                msg.rol === 'usuario' ? (
                  <div key={msg.id} className="flex justify-end">
                    <div className="max-w-[85%] rounded-2xl bg-[#5fd3a5] px-4 py-3 font-medium text-[#0d2a1e]">
                      {msg.texto}
                    </div>
                  </div>
                ) : (
                  <div key={msg.id} className="flex justify-start">
                    <div className="max-w-[85%] rounded-2xl border border-[#26292d] bg-[#1e2124] px-4 py-3 leading-relaxed">
                      {msg.texto ? (
                        renderMarkdown(msg.texto)
                      ) : !msg.error ? (
                        <span className="animate-pulse text-[#9ba1a6]">{msg.estado}</span>
                      ) : null}

                      {msg.error && <p className="mt-2 text-red-300">{msg.error}</p>}

                      {msg.degradado && (
                        <p className="mt-3 text-xs text-amber-300">
                          Respuesta resumida: el servicio de redacción estaba saturado.
                        </p>
                      )}

                      {msg.fuentes?.length > 0 && (
                        <p className="mt-3 border-t border-[#26292d] pt-2 text-xs text-[#9ba1a6]">
                          Fuentes: {[...new Set(msg.fuentes.map((f) => f.fuente))].join(', ')}
                        </p>
                      )}
                    </div>
                  </div>
                )
              )
            )}

            {/* Preguntas sugeridas por el asistente */}
            {!loading && ultimo?.rol === 'ia' && ultimo.sugerencias?.length > 0 && (
              <div className="flex flex-wrap gap-2 pt-1">
                {ultimo.sugerencias.map((s) => (
                  <button
                    key={s}
                    onClick={() => enviar(s)}
                    className="rounded-full border border-[#26292d] bg-[#1e2124] px-3 py-1.5 text-left text-sm text-[#5fd3a5] transition-colors hover:border-[#3fbf8f] focus-visible:outline-2 focus-visible:outline-[#3fbf8f]"
                  >
                    {s}
                  </button>
                ))}
              </div>
            )}
            <div ref={finRef} />
          </div>
        </div>

        {/* Input */}
        <footer className="px-4 pb-3 pt-2">
          <form
            onSubmit={handleSubmit}
            className="mx-auto flex w-full max-w-3xl items-center gap-2 rounded-full border border-[#26292d] bg-[#1e2124] p-2 pl-5"
          >
            <input
              type="text"
              value={pregunta}
              onChange={(e) => setPregunta(e.target.value)}
              placeholder="Escribe tu consulta sobre normativa tributaria..."
              className="flex-1 bg-transparent text-[#e6e8ea] placeholder:text-[#9ba1a6] focus:outline-none"
            />
            <button
              type="submit"
              disabled={loading || !pregunta.trim()}
              className="rounded-full bg-[#3fbf8f] px-5 py-2 font-medium text-[#0d2a1e] transition-opacity hover:opacity-90 disabled:opacity-40"
            >
              Enviar
            </button>
          </form>
          <p className="mt-2 text-center text-xs text-[#9ba1a6]">
            El asistente puede cometer errores. Verifica la normativa citada antes de aplicarla.
          </p>
        </footer>
      </main>
    </div>
  );
}
