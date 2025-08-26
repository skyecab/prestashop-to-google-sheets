/** ================== CONFIG & CAMPOS ================== */

const CONFIG = {
  get TOKEN() {
    const v = PropertiesService.getScriptProperties().getProperty('PSHOP_TOKEN');
    return (v || '').trim();
  },
  PAGE_SIZE: 100,
  // Tiempo de espera base para backoff (ms)
  BASE_BACKOFF_MS: 800
};

const FIELDS = {
  orders: '[id,reference,date_add,total_paid,payment,current_state,id_customer,total_products,total_products_wt,id_address_delivery,total_shipping,total_discounts]',
  customers: '[id,firstname,lastname,email,date_add,date_upd]',
  addresses: '[id,id_customer,alias,address1,address2,city,postcode,phone,phone_mobile,id_country]',
  order_carriers: '[id_order,weight]'
};

/** ================== HELPERS API ================== */
function buildBase_(path) {
  const BASE = (PropertiesService.getScriptProperties().getProperty('BASE_URL') || '')
    .trim()
    .replace(/\/+$/,'');
  if (!BASE) throw new Error('BASE_URL no está configurado en Propiedades del Script.');
  return `${BASE}/${path.replace(/^\/+/, '')}`;
}

function fetchJson_(url) {
  const KEY = CONFIG.TOKEN;
  const sep = url.includes('?') ? '&' : '?';
  const finalUrl = url.includes('ws_key=') ? url : `${url}${sep}ws_key=${encodeURIComponent(KEY)}`;

  const opts = {
    method: 'get',
    muteHttpExceptions: true,
    followRedirects: true,
    headers: { Accept: 'application/json' }
  };

  let lastErr = null;
  for (let i = 0; i < 3; i++) {
    const resp = UrlFetchApp.fetch(finalUrl, opts);
    const code = resp.getResponseCode();
    const ctype = String(resp.getHeaders()['Content-Type'] || '').toLowerCase();
    const body = resp.getContentText();

    if (code === 200) {
      const trimmed = body.trim();
      const looksJson = ctype.includes('json') || trimmed.startsWith('{') || trimmed.startsWith('[');
      if (!looksJson) {
        throw new Error(
            'Respuesta 200 pero no es JSON (¿BASE_URL sin /api, WAF o permisos?).' +
            `\nURL: ${redactWsKey_(finalUrl)}` +
            `\nContent-Type: ${ctype}` +
            `\nSnippet: ${trimmed.slice(0, 200)}`
        );
      }
      return JSON.parse(trimmed);
    }
    
    if (code >= 500) {
      Utilities.sleep(800 * (i + 1));
      lastErr = `HTTP ${code}: ${body.slice(0,180)}`;
      continue;
    }

    throw new Error(
      `HTTP ${code} al llamar a PrestaShop.` +
      `\nURL: ${redactWsKey_(finalUrl)}` +
      `\nSnippet: ${body.slice(0, 200)}`
    );
  }
  throw new Error(lastErr || 'Error desconocido al llamar a PrestaShop.');
}

function redactWsKey_(u) {
  return u.replace(/(ws_key=)[^&]+/i, '$1***');
}

function apiGetPaged_({ recurso, fields, sort, offset, pageSize }) {
  const url = buildBase_(recurso)
    + `?display=${encodeURIComponent(fields)}`
    + (sort ? `&sort=${encodeURIComponent(sort)}` : '')
    + `&limit=${offset},${pageSize}`
    + `&output_format=JSON&io_format=JSON`;
  return fetchJson_(url);
}

/** ================== HELPERS SHEETS ================== */
function ensureSheet_(name) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  return ss.getSheetByName(name) || ss.insertSheet(name);
}

// Sincroniza encabezados con lo que entrega la API hoy
function getOrCreateHeaders_({ sheet, recurso, fields, claveLista }) {
  if (sheet.getLastRow() === 0) {
    const sample = apiGetPaged_({ recurso, fields, sort: '[id_DESC]', offset: 0, pageSize: 1 });
    const arr = sample[claveLista] || [];
    const obj = arr[0] || {};
    const headers = Object.keys(obj);
    if (headers.length) sheet.appendRow(headers);
    return headers;
  }

  // EXISTE fila de headers: leer los actuales…
  const current = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

  // …y comparar con lo que devuelve la API hoy
  const sample = apiGetPaged_({ recurso, fields, sort: '[id_DESC]', offset: 0, pageSize: 1 });
  const arr = sample[claveLista] || [];
  const obj = arr[0] || {};
  const sampleKeys = Object.keys(obj);

  // Agregar al final cualquier campo nuevo que no esté todavía (p.ej. total_shipping/total_discounts)
  const missing = sampleKeys.filter((k) => !current.includes(k));
  if (missing.length) {
    let last = sheet.getLastColumn();
    missing.forEach((k) => {
      sheet.insertColumnAfter(last);
      sheet.getRange(1, last + 1).setValue(k);
      last++;
    });
    return sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  }

  return current;
}


function safeNotify_(msg) {
  try {
    SpreadsheetApp.getUi().alert(msg);
  } catch (_) {
    Logger.log(msg);
  }
}

/** ================== FETCH POR LOTES (IDs) ================== */
function fetchByIds_({ recurso, fields, ids, claveLista }) {
  const out = [];
  if (!ids || !ids.length) return out;

  const batchSize = 40; // prudente para URLs + límites
  const unique = Array.from(new Set(ids.map(String)));

  for (let i = 0; i < unique.length; i += batchSize) {
    const slice = unique.slice(i, i + batchSize);
    const lote = slice.join('|');
    const base = buildBase_(recurso);
    const url =
      base +
      `?display=${encodeURIComponent(fields)}` +
      `&filter[id]=[${encodeURIComponent(lote)}]` +
      `&limit=0,${slice.length}` +
      `&output_format=JSON&io_format=JSON`;

    let attempts = 0;
    while (true) {
      try {
        const data = fetchJson_(url);
        const arr = data[claveLista] || [];
        out.push(...arr);
        // Pequeño throttle entre lotes
        Utilities.sleep(250);
        break;
      } catch (e) {
        attempts++;
        const msg = String(e);
        if (
          attempts <= 4 &&
          (msg.includes('Bandwidth quota exceeded') || msg.includes('HTTP 429') || msg.match(/HTTP\s5\d{2}/))
        ) {
          const wait = CONFIG.BASE_BACKOFF_MS * attempts + Math.floor(Math.random() * 400);
          Utilities.sleep(wait);
          continue;
        }
        throw e;
      }
    }
  }
  return out;
}


/** ================== PESO (order_carriers) ================== */
function buildWeightMapByOrderIds_(orderIds) {
  const map = {};
  if (!orderIds || !orderIds.length) return map;

  const unique = Array.from(new Set(orderIds.map(String)));
  const batchSize = 15; // bajo para evitar límites

  for (let i = 0; i < unique.length; i += batchSize) {
    const slice = unique.slice(i, i + batchSize);
    const lote = slice.join('|');
    const url =
      buildBase_('order_carriers') +
      `?display=${encodeURIComponent(FIELDS.order_carriers)}` +
      `&filter[id_order]=[${encodeURIComponent(lote)}]` +
      `&limit=0,${slice.length}` +
      `&output_format=JSON&io_format=JSON`;

    // reintentos con backoff si el host limita (bandwidth / 429 / 5xx)
    let attempts = 0;
    while (true) {
      try {
        const data = fetchJson_(url);
        const arr = (data && data.order_carriers) ? data.order_carriers : [];
        arr.forEach((oc) => {
          const k = String(oc.id_order);
          if (map[k] == null || map[k] === '') {
            const w = parseFloat(oc.weight);
            if (isFinite(w) && w > 0) map[k] = w.toFixed(3);
          }
        });
        Utilities.sleep(350); // breve pausa para no saturar
        break;
      } catch (e) {
        attempts++;
        const msg = String(e);
        if (
          attempts <= 5 &&
          (msg.includes('Bandwidth quota exceeded') || msg.includes('HTTP 429') || msg.match(/HTTP\s5\d{2}/))
        ) {
          const wait = CONFIG.BASE_BACKOFF_MS * attempts + Math.floor(Math.random() * 400);
          Utilities.sleep(wait);
          continue;
        }
        throw e;
      }
    }
  }
  return map;
}


/** ================== HELPERS IDEMPOTENCIA ================== */
function _isBlank_(v) {
  return v == null || String(v).trim() === '';
}

function _isZeroLike_(v) {
  const s = String(v).trim().toLowerCase();
  if (s === '' || s === 'nan') return true;
  const n = Number(s);
  return isFinite(n) ? n === 0 : false;
}

function _parseDateSafe_(s) {
  if (!s || typeof s !== 'string') return null;
  // Formato típico PS: 'YYYY-MM-DD HH:mm:ss'
  const parts = s.replace('T', ' ').split(/[\s:-]/).map(Number);
  if (parts.length < 6) return null;
  const d = new Date(parts[0], parts[1] - 1, parts[2], parts[3], parts[4], parts[5]);
  return isNaN(d.getTime()) ? null : d;
}

// ¿El nuevo registro es más fresco que el actual?
function isFresher_(currentDateStr, newDateStr) {
  const a = _parseDateSafe_(currentDateStr);
  const b = _parseDateSafe_(newDateStr);
  if (!a && b) return true;   // no había fecha, llega una válida
  if (!b) return false;       // nuevo sin fecha no es más fresco
  return b.getTime() >= a.getTime();
}

// Política: no sobreescribir con vacío; opcionalmente considerar 0 como vacío.
function chooseValue_(current, incoming, { treatZeroAsEmpty = false } = {}) {
  const currIsEmpty = _isBlank_(current) || (treatZeroAsEmpty && _isZeroLike_(current));
  const nextIsEmpty = _isBlank_(incoming) || (treatZeroAsEmpty && _isZeroLike_(incoming));
  if (currIsEmpty && !nextIsEmpty) return incoming;
  if (!currIsEmpty && nextIsEmpty) return current; // no degradar
  // Ambos presentes (o ambos vacíos): preferir incoming (caller decide si chequea frescura antes)
  return nextIsEmpty ? current : incoming;
}

// Igual que arriba pero asegurando número válido
function chooseNumeric_(current, incoming, { treatZeroAsEmpty = false } = {}) {
  const chosen = chooseValue_(current, incoming, { treatZeroAsEmpty });
  const n = Number(chosen);
  return isFinite(n) ? chosen : current;
}

// Emails válidos mínimos (para no degradar)
function isValidEmail_(s) {
  if (!s) return false;
  const r = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return r.test(String(s).trim());
}

/** ================== MAP DE ESTADOS ================== */
const ORDER_STATES = {
  1: 'Pago pendiente',
  2: 'Pago aceptado',
  3: 'Preparación en proceso',
  4: 'Enviado',
  5: 'Entregado',
  6: 'Cancelado',
  7: 'Reembolso',
  8: 'Error en el pago',
  9: 'Productos fuera de línea',
  10: 'Pago por transferencia bancaria pendiente',
  11: 'Pago mediante PayPal pendiente',
  12: 'Pago a distancia aceptado',
  13: 'Productos fuera de línea',
  14: 'Awaiting COD validation',
  15: 'Pedido al proveedor',
  19: 'Compra en tienda física',
  20: 'Pago por Link de Pago Pendiente',
  22: 'En espera de pago',
  24: 'Listo para ABC',
  25: 'En espera de Link de Pago',
  26: 'En espera de Recojo',
  27: 'En consignación',
  28: 'Entregado - Pendiente de Pago'
};
