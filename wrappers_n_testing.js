/** ========= WRAPPERS ADMIN =========
 * Ejecuta desde el Editor: Run -> Run function
 *
 * Funciones principales:
 * - runBackfillTotals(limit, dryRun)
 * - runBackfillTotalsFromId(minId, limit, dryRun)
 * - runBackfillTotalsInRange(minId, maxId, limit, dryRun)
 * - runBackfillWeights(limit, dryRun)
 * - runBackfillWeightsFromId(minId, limit, dryRun)
 * - runBackfillWeightsInRange(minId, maxId, limit, dryRun)
 * - resetBackfillProgress()
 */

/* ===================== UTILIDADES LOCALES ===================== */

function _getSheetAndHeaders_(name) {
  const sheet = ensureSheet_(name);
  const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
  return { sheet, headers };
}

function _idx_(headers, name) {
  const i = headers.indexOf(name);
  if (i === -1) throw new Error(`Falta columna "${name}" en hoja.`);
  return i;
}

function _colVals_(sheet, colIdx0, nRows) {
  return sheet.getRange(2, colIdx0+1, nRows, 1).getValues().flat();
}

function _setColVals_(sheet, colIdx0, values) {
  sheet.getRange(2, colIdx0+1, values.length, 1).setValues(values.map(v => [v]));
}

function _countMissing_(arr) {
  let c = 0; for (let i=0;i<arr.length;i++) if (!arr[i] || String(arr[i]).trim()==='') c++; return c;
}

function _withinRange_(idStr, minId, maxId) {
  const n = Number(idStr);
  if (!isFinite(n)) return false;
  if (minId != null && n < minId) return false;
  if (maxId != null && n > maxId) return false;
  return true;
}

/* ===================== TOTALS (shipping/discounts) ===================== */

/** Backfill totals por tandas (sin filtro de id). */
function runBackfillTotals(limit, dryRun) {
  return runBackfillTotalsInRange(null, null, limit, dryRun);
}

/** Backfill totals empezando desde minId (inclusive). */
function runBackfillTotalsFromId(minId, limit, dryRun) {
  return runBackfillTotalsInRange(Number(minId)||0, null, limit, dryRun);
}

/** Backfill totals en rango [minId..maxId] (inclusive). */
function runBackfillTotalsInRange(minId, maxId, limit, dryRun) {
  const LIM = Number(limit) > 0 ? Number(limit) : 300;
  const isDry = String(dryRun) === 'true';

  const { sheet, headers } = _getSheetAndHeaders_('Órdenes');
  const idxId   = _idx_(headers, 'id');
  const idxShip = headers.indexOf('total_shipping'); // puede no existir
  const idxDisc = headers.indexOf('total_discounts'); // puede no existir
  if (idxShip === -1 && idxDisc === -1) return safeNotify_('No hay columnas total_shipping/total_discounts.');

  const n = sheet.getLastRow() - 1;
  if (n <= 0) return safeNotify_('No hay filas.');

  const ids  = _colVals_(sheet, idxId, n).map(String);
  const ship = idxShip !== -1 ? _colVals_(sheet, idxShip, n) : null;
  const disc = idxDisc !== -1 ? _colVals_(sheet, idxDisc, n) : null;

  // Selecciona hasta LIM ids del rango con campos faltantes
  const toFetch = [];
  for (let i=0; i<n && toFetch.length < LIM; i++) {
    const inRange = _withinRange_(ids[i], minId, maxId);
    if (!inRange) continue;
    const needShip = ship && (!ship[i] || ship[i] === '');
    const needDisc = disc && (!disc[i] || disc[i] === '');
    if (needShip || needDisc) toFetch.push(ids[i]);
  }
  if (!toFetch.length) return safeNotify_('No hay totals faltantes para el criterio.');

  const fetched = fetchByIds_({
    recurso: 'orders',
    fields: FIELDS.orders,
    ids: Array.from(new Set(toFetch)),
    claveLista: 'orders'
  });

  const map = {}; fetched.forEach(o => { map[String(o.id)] = o; });

  let updates = 0;
  if (!isDry) {
    if (ship) {
      const out = ship.slice();
      for (let i=0; i<n; i++) {
        if (!_withinRange_(ids[i], minId, maxId)) continue;
        if (!out[i] || out[i] === '') {
          const o = map[ids[i]];
          if (o && o.total_shipping != null && o.total_shipping !== '') { out[i] = o.total_shipping; updates++; }
        }
      }
      if (updates) _setColVals_(sheet, idxShip, out);
    }
    if (disc) {
      const cur = disc.slice();
      let upd2 = 0;
      for (let i=0; i<n; i++) {
        if (!_withinRange_(ids[i], minId, maxId)) continue;
        if (!cur[i] || cur[i] === '') {
          const o = map[ids[i]];
          if (o && o.total_discounts != null && o.total_discounts !== '') { cur[i] = o.total_discounts; upd2++; }
        }
      }
      if (upd2) _setColVals_(sheet, idxDisc, cur);
      updates += upd2;
    }
  }

  const msg = isDry
    ? `DRY RUN totals: se analizarían ${toFetch.length} órdenes en el rango.`
    : `Backfill totals: ${updates} celdas actualizadas (rango ${minId ?? '-'}..${maxId ?? '-'})`;
  safeNotify_(msg);
  return updates;
}

/* ===================== WEIGHT (order_carriers) ===================== */

/** Backfill weights por tandas (sin filtro de id). */
function runBackfillWeights(limit, dryRun) {
  return runBackfillWeightsInRange(null, null, limit, dryRun);
}

/** Backfill weights desde minId (inclusive). */
function runBackfillWeightsFromId(minId, limit, dryRun) {
  return runBackfillWeightsInRange(Number(minId)||0, null, limit, dryRun);
}

/** Backfill weights en rango [minId..maxId] (inclusive). */
function runBackfillWeightsInRange(minId, maxId, limit, dryRun) {
  const MAX_IDS = Number(limit) > 0 ? Number(limit) : 120;
  const isDry = String(dryRun) === 'true';

  const { sheet, headers } = _getSheetAndHeaders_('Órdenes');
  const idxId = _idx_(headers, 'id');
  const idxW  = _idx_(headers, 'weight');

  const n = sheet.getLastRow() - 1;
  if (n <= 0) return safeNotify_('No hay filas.');

  const ids  = _colVals_(sheet, idxId, n).map(String);
  const w    = _colVals_(sheet, idxW,  n);

  // Toma hasta MAX_IDS ids en rango y sin peso
  const targets = [];
  for (let i=0; i<n && targets.length < MAX_IDS; i++) {
    if (!w[i] || String(w[i]).trim()==='') {
      if (_withinRange_(ids[i], minId, maxId)) targets.push(ids[i]);
    }
  }
  if (!targets.length) return safeNotify_('No hay pesos faltantes para el criterio.');

  let updates = 0;
  if (!isDry) {
    const map = buildWeightMapByOrderIds_(Array.from(new Set(targets))) || {};
    const out = w.slice();
    for (let i=0; i<n; i++) {
      if (!out[i] || String(out[i]).trim()==='') {
        if (_withinRange_(ids[i], minId, maxId)) {
          const val = map[ids[i]];
          if (val) { out[i] = val; updates++; }
        }
      }
    }
    if (updates) _setColVals_(sheet, idxW, out);
  }

  const msg = isDry
    ? `DRY RUN weights: se analizarían ${targets.length} órdenes en el rango.`
    : `Backfill weight: ${updates} filas actualizadas (rango ${minId ?? '-'}..${maxId ?? '-'})`;
  safeNotify_(msg);
  return updates;
}

/* ===================== OTROS ===================== */

function resetBackfillProgress() {
  PropertiesService.getScriptProperties().deleteProperty('backfill_row');
  safeNotify_('Checkpoint de backfill reseteado.');
}

// Wrapper para ejecución aquí
function runSelectedFunction() {
  //runBackfillTotals(limit, dryRun)
  //runBackfillTotalsFromId(minId, limit, dryRun)
  //runBackfillTotalsInRange(2, 233, 300, false)
  //runBackfillWeights(limit, dryRun)
  //runBackfillWeightsFromId(minId, limit, dryRun)
  //runBackfillWeightsInRange(minId, maxId, limit, dryRun)
  //resetBackfillProgress()
}

