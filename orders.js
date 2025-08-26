function actualizarOrdenes() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) { Logger.log('Saltando: hay otra ejecución.'); return; }
  try {
    syncOrdenes_({ soloNuevas: true });
  } finally {
    lock.releaseLock();
  }
}

/**
* Sincroniza órdenes desde PrestaShop hacia la hoja "Órdenes".
* - Inserta nuevas al inicio (debajo del header).
* - Asegura columnas adicionales: estado_nombre, weight (y valida presencia de shipping/discounts).
* - Refresca massivo de estado_nombre para todas las filas.
* - Enriquecer peso desde /order_carriers para nuevas y backfill para filas con weight vacío (límite configurable).
* - Dispara traerClientes() y traerDirecciones() cuando hay nuevas filas.
*/

function syncOrdenes_(opciones) {
  const hoja = 'Órdenes';
  const sheet = ensureSheet_(hoja);
  const pageSize = CONFIG.PAGE_SIZE || 100;
  const opts = Object.assign({ soloNuevas: true, diasAtras: null, backfillWeightLimit: 0 }, opciones || {});

   // 1) Encabezados base desde la API
  let headers = getOrCreateHeaders_({ sheet, recurso: 'orders', fields: FIELDS.orders, claveLista: 'orders' });
  if (headers.length === 0) return safeNotify_('No se pudieron obtener encabezados (¿sin órdenes?).');

  // 1.1) Asegurar columnas adicionales
  headers = ensureExtraColumns_(sheet, headers, ['estado_nombre', 'weight', 'total_shipping', 'total_discounts']);

  // 2) Último ID (para solo nuevas)
  let lastId = 0;
  if (opciones.soloNuevas) {
    const idColIndex = headers.indexOf('id') + 1;
    if (sheet.getLastRow() > 1 && idColIndex > 0) {
      const ids = sheet.getRange(2, idColIndex, sheet.getLastRow() - 1, 1).getValues()
        .flat().filter(v => v !== '' && !isNaN(v));
      if (ids.length > 0) lastId = Math.max(...ids.map(Number));
    }
  }

  // 3) Límite por días (opcional)
  let fechaLimite = null;
  if (opciones.diasAtras) {
    const hoy = new Date();
    fechaLimite = new Date(hoy.getTime());
    fechaLimite.setDate(hoy.getDate() - opciones.diasAtras);
  }

  // 4) Descarga incremental (ordenado por id_DESC)
  const rowsToPrepend = [];
  const newOrderIds = [];
  let offset = 0, stop = false;

  while (!stop) {
    const data = apiGetPaged_({ 
      recurso: 'orders', 
      fields: FIELDS.orders, 
      sort: '[id_DESC]', 
      offset, 
      pageSize });
    const orders = data.orders || [];
    if (orders.length === 0) break;

    for (const o of orders) {
      const oid = Number(o.id);
      if (opciones.soloNuevas && oid <= lastId) { stop = true; break; }
      if (fechaLimite) {
        const fechaOrden = new Date(o.date_add);
        if (fechaOrden < fechaLimite) { stop = true; break; }
      }

      // 4.1) Construir fila con headers actuales
      const row = headers.map(h => {
      if (h === 'estado_nombre') return ''; // se llena más abajo
      if (h === 'weight') return ''; // se llenará desde /order_carriers
      return (o[h] != null ? o[h] : '');
      });

      // 4.2) Traducción de estado
      const idxEstadoNom = headers.indexOf('estado_nombre');
      const estadoTrad = ORDER_STATES[o.current_state] || (o.current_state !== undefined ? `Estado ${o.current_state}` : '');
      if (idxEstadoNom >= 0) row[idxEstadoNom] = estadoTrad;

      rowsToPrepend.push(row);
      newOrderIds.push(String(oid));
    }
    
    if (stop) break;
    offset += pageSize;
  }

  // 5) Inserta nuevas (si hay)(debajo del encabezado)
  if (rowsToPrepend.length) {
    sheet.insertRowsBefore(2, rowsToPrepend.length);
    sheet.getRange(2, 1, rowsToPrepend.length, headers.length).setValues(rowsToPrepend);
  }

  // 6) REFRESH masivo de 'estado_nombre' para TODAS las filas
  if (sheet.getLastRow() > 1) {
    const idxEstado   = headers.indexOf('current_state');
    const idxEstadoNm = headers.indexOf('estado_nombre');
    const nRows = sheet.getLastRow() - 1;

    if (idxEstado !== -1 && idxEstadoNm !== -1) {
      const estados = sheet.getRange(2, idxEstado + 1, nRows, 1).getValues().flat();
      const traducidos = estados.map(v => {
        const k = Number(v);
        return ORDER_STATES[k] || (v !== '' ? `Estado ${v}` : '');
      });
      // escribe solo la columna estado_nombre
      sheet.getRange(2, idxEstadoNm + 1, nRows, 1)
        .setValues(traducidos.map(x => [x]));
    }
  }

  // 6) Refresh masivo de 'estado_nombre' para TODAS las filas (asegura consistencia)
  refreshEstadoNombre_(sheet, headers);

  // 7) Enriquecimiento de WEIGHT desde /order_carriers
  // 7.1) Backfill para nuevas órdenes
  const filledNew = fillWeightForIds_(sheet, headers, newOrderIds);

  // 7.2) Backfill adicional (opcional) para filas existentes con weight vacío (hasta límite)
  let filledExisting = 0;
  if (opts.backfillWeightLimit && opts.backfillWeightLimit > 0) {
  filledExisting = backfillMissingWeights_(sheet, headers, opts.backfillWeightLimit);
  }

  // 8) Enriquecer Clientes/Direcciones solo si hubo nuevas
  if (rowsToPrepend.length) {
    try { if (typeof traerClientes === 'function') traerClientes(); } catch (e) { Logger.log('traerClientes error: ' + e); }
    try { if (typeof traerDirecciones === 'function') traerDirecciones(); } catch (e) { Logger.log('traerDirecciones error: ' + e); }
  }

  // 9) Notificación final
  const msg = [];
  if (rowsToPrepend.length) msg.push(`Órdenes nuevas: ${rowsToPrepend.length}`);
  msg.push('Estado actualizado');
  if (filledNew) msg.push(`Peso nuevas: ${filledNew}`);
  if (filledExisting) msg.push(`Peso backfill: ${filledExisting}`);
}


/** Asegura columnas extras; si no existen, las agrega al final y retorna el nuevo arreglo de headers **/
function ensureExtraColumns_(sheet, headers, extraCols) {
  const current = headers.slice();
  let changed = false;
  extraCols.forEach(col => {
    if (!current.includes(col)) {
      const last = sheet.getLastColumn();
      sheet.insertColumnAfter(last);
      sheet.getRange(1, last + 1).setValue(col);
      current.push(col);
      changed = true;
    }
  });
  return changed ? sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0] : current;
}

/** Refresca la columna estado_nombre para todas las filas de la hoja */
function refreshEstadoNombre_(sheet, headers) {
  if (sheet.getLastRow() <= 1) return;
  const idxEstado = headers.indexOf('current_state');
  const idxEstadoNm = headers.indexOf('estado_nombre');
  const nRows = sheet.getLastRow() - 1;
  if (idxEstado === -1 || idxEstadoNm === -1) return;
  const estados = sheet.getRange(2, idxEstado + 1, nRows, 1).getValues().flat();
  const traducidos = estados.map(v => {
    const k = Number(v);
    return ORDER_STATES[k] || (v !== '' ? `Estado ${v}` : '');
  });
  sheet.getRange(2, idxEstadoNm + 1, nRows, 1).setValues(traducidos.map(x => [x]));
}


/** Rellena weight para un conjunto de orderIds usando /order_carriers; retorna cuántas filas fueron actualizadas */
function fillWeightForIds_(sheet, headers, orderIds) {
  if (!orderIds || !orderIds.length) return 0;
  const idxId = headers.indexOf('id');
  const idxW = headers.indexOf('weight');
  if (idxId === -1 || idxW === -1) return 0;

  // Mapa desde la API
  const weightMap = buildWeightMapByOrderIds_(Array.from(new Set(orderIds)));
  if (!weightMap || Object.keys(weightMap).length === 0) return 0;


  // Recorremos todas las filas y actualizamos solo ids coincidentes
  const nRows = sheet.getLastRow() - 1;
  if (nRows <= 0) return 0;
  const idVals = sheet.getRange(2, idxId + 1, nRows, 1).getValues().flat().map(String);
  const wVals = sheet.getRange(2, idxW + 1, nRows, 1).getValues().flat();

  let updates = 0;
  const out = wVals.slice();
  for (let i = 0; i < nRows; i++) {
    const oid = idVals[i];
    if (orderIds.indexOf(oid) !== -1) {
      const w = weightMap[oid];
      if (w != null && w !== '' && out[i] !== w) { out[i] = w; updates++; }
    }
  }
  if (updates > 0) sheet.getRange(2, idxW + 1, nRows, 1).setValues(out.map(v => [v]));
  return updates;
}


/** Busca filas con weight vacío y hace backfill hasta un límite; retorna cuántas filas actualizó */
function backfillMissingWeights_(sheet, headers, limit) {
  if (sheet.getLastRow() <= 1) return 0;
  const idxId = headers.indexOf('id');
  const idxW = headers.indexOf('weight');
  if (idxId === -1 || idxW === -1) return 0;
  const nRows = sheet.getLastRow() - 1;
  const idVals = sheet.getRange(2, idxId + 1, nRows, 1).getValues().flat().map(String);
  const wVals = sheet.getRange(2, idxW + 1, nRows, 1).getValues().flat();

  const missingIds = [];
  for (let i = 0; i < nRows; i++) {
    if ((wVals[i] == null || wVals[i] === '') && idVals[i]) missingIds.push(idVals[i]);
    if (missingIds.length >= limit) break;
  }
  if (!missingIds.length) return 0;

  const map = buildWeightMapByOrderIds_(Array.from(new Set(missingIds)));
  if (!map || Object.keys(map).length === 0) return 0;

  let updates = 0;
  const out = wVals.slice();
  for (let i = 0; i < nRows; i++) {
    const oid = idVals[i];
    if ((out[i] == null || out[i] === '') && map[oid] != null && map[oid] !== '') {
    out[i] = map[oid];
    updates++;
    }
  }
  if (updates > 0) sheet.getRange(2, idxW + 1, nRows, 1).setValues(out.map(v => [v]));
  return updates;
}

function backfillTotalsForMissing_(limit) {
  const sheet = ensureSheet_('Órdenes');
  if (sheet.getLastRow() <= 1) return 0;
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  const idxId   = headers.indexOf('id');
  const idxShip = headers.indexOf('total_shipping');
  const idxDisc = headers.indexOf('total_discounts');
  if (idxId === -1 || (idxShip === -1 && idxDisc === -1)) return 0;

  const n = sheet.getLastRow() - 1;
  const ids  = sheet.getRange(2, idxId+1, n, 1).getValues().flat().map(String);
  const ship = idxShip !== -1 ? sheet.getRange(2, idxShip+1, n, 1).getValues().flat() : null;
  const disc = idxDisc !== -1 ? sheet.getRange(2, idxDisc+1, n, 1).getValues().flat() : null;

  const missingIds = [];
  for (let i=0; i<n; i++) {
    const needShip = ship && (ship[i] == null || ship[i] === '');
    const needDisc = disc && (disc[i] == null || disc[i] === '');
    if ((needShip || needDisc) && ids[i]) missingIds.push(ids[i]);
    if (limit && missingIds.length >= limit) break;
  }
  if (!missingIds.length) return 0;

  const fetched = fetchByIds_({
    recurso: 'orders',
    fields: FIELDS.orders, // ya incluye total_shipping/total_discounts
    ids: Array.from(new Set(missingIds)),
    claveLista: 'orders'
  });
  const map = {};
  fetched.forEach(o => { map[String(o.id)] = o; });

  let updates = 0;
  for (let i=0; i<n; i++) {
    const oid = ids[i];
    const o = map[oid];
    if (!o) continue;
    if (ship && (ship[i] == null || ship[i] === '') && o.total_shipping != null && o.total_shipping !== '') {
      ship[i] = o.total_shipping; updates++;
    }
    if (disc && (disc[i] == null || disc[i] === '') && o.total_discounts != null && o.total_discounts !== '') {
      disc[i] = o.total_discounts; updates++;
    }
  }
  if (updates) {
    if (ship) sheet.getRange(2, idxShip+1, n, 1).setValues(ship.map(v => [v]));
    if (disc) sheet.getRange(2, idxDisc+1, n, 1).setValues(disc.map(v => [v]));
  }
  safeNotify_(`Backfill totals: ${updates} celdas actualizadas.`);
  return updates;
}
