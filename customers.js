function traerClientes() {
  const shOrd = ensureSheet_('Órdenes');
  if (shOrd.getLastRow() < 2) return safeNotify_('Órdenes está vacío.');

  const headersOrd = shOrd.getRange(1,1,1,shOrd.getLastColumn()).getValues()[0];
  const idxCust = headersOrd.indexOf('id_customer');
  if (idxCust === -1) return safeNotify_('No encuentro id_customer en Órdenes.');

  const ids = shOrd.getRange(2, idxCust+1, shOrd.getLastRow()-1, 1).getValues()
    .flat().filter(v => v !== '' && v !== null).map(String);
  const idsUnicos = [...new Set(ids)];
  if (!idsUnicos.length) return safeNotify_('No hay id_customer en Órdenes.');

  // Descarga por lotes solo los clientes usados en Órdenes
  const lista = fetchByIds_({
    recurso: 'customers',
    fields: FIELDS.customers,
    ids: idsUnicos,
    claveLista: 'customers'
  });

  // Definimos headers finales (incluye campos calculados)
  const headers = ['id','firstname','lastname','email','date_add','date_upd','nombre_completo','razon_social'];

  // Normalizamos filas nuevas
  const newRows = lista.map(c => {
    const fn = c.firstname || '', ln = c.lastname || '';
    const full = (fn + ' ' + ln).trim();
    const razon = (c.company && String(c.company).trim()) ? c.company : full;
    return [c.id||'', fn, ln, c.email||'', c.date_add||'', c.date_upd||'', full, razon];
  });

  const shCli = ensureSheet_('Clientes');

  // Si la hoja está vacía, solo escribimos headers + newRows
  if (shCli.getLastRow() === 0) {
    shCli.appendRow(headers);
    if (newRows.length) shCli.getRange(2, 1, newRows.length, headers.length).setValues(newRows);
    safeNotify_(`Clientes: ${newRows.length} filas nuevas.`);
    return;
  }

  // Si ya hay datos, hacemos UPSERT por id
  const existingHeaders = shCli.getRange(1,1,1,shCli.getLastColumn()).getValues()[0];

  // Aseguramos que los headers coincidan; si no, reescribimos encabezado (una sola vez)
  const headersChanged = existingHeaders.join('|') !== headers.join('|');
  if (headersChanged) {
    // Leemos las filas existentes y tratamos de remapear columnas si el header cambió
    const existingValues = (shCli.getLastRow() > 1)
      ? shCli.getRange(2, 1, shCli.getLastRow() - 1, existingHeaders.length).getValues()
      : [];
    // Construimos un índice de columnas existentes
    const colIndex = Object.fromEntries(existingHeaders.map((h, i) => [h, i]));

    // Remapeamos existentes al nuevo orden de headers (si falta un campo, lo dejamos vacío)
    const remappedExisting = existingValues.map(r =>
      headers.map(h => (colIndex[h] != null ? r[colIndex[h]] : ''))
    );

    // Ahora combinamos: existentes en map por id, luego pisamos con newRows
    const idIdx = headers.indexOf('id');
    const byId = new Map(remappedExisting.map(r => [String(r[idIdx]), r]));
    for (const row of newRows) byId.set(String(row[idIdx]), row);

    // Reescribimos hoja completa (headers + todas las filas)
    shCli.clearContents();
    shCli.appendRow(headers);
    const merged = Array.from(byId.values());
    if (merged.length) shCli.getRange(2, 1, merged.length, headers.length).setValues(merged);

    safeNotify_(`Clientes: ${newRows.length} filas nuevas/actualizadas, total ${merged.length}.`);
    return;
  }

  // Headers coinciden: upsert directo
  const idIdx = headers.indexOf('id');
  const existing = new Map();
  if (shCli.getLastRow() > 1) {
    const existingValues = shCli.getRange(2, 1, shCli.getLastRow() - 1, headers.length).getValues();
    for (const r of existingValues) existing.set(String(r[idIdx]), r);
  }

  for (const row of newRows) {
    existing.set(String(row[idIdx]), row); // pisa si existe, inserta si no
  }

  // Escribimos todo de nuevo (mantiene orden por última actualización)
  const merged = Array.from(existing.values());
  shCli.clearContents();
  shCli.appendRow(headers);
  if (merged.length) shCli.getRange(2, 1, merged.length, headers.length).setValues(merged);

  safeNotify_(`Clientes: ${newRows.length} filas nuevas/actualizadas, total ${merged.length}.`);
}
