function traerDirecciones() {
  // === 0) Validaciones iniciales
  const shOrd = ensureSheet_('Órdenes');
  if (shOrd.getLastRow() < 2) return safeNotify_('Órdenes está vacío.');

  const headersOrd = shOrd.getRange(1,1,1,shOrd.getLastColumn()).getValues()[0];
  const idxAddr = headersOrd.indexOf('id_address_delivery');
  if (idxAddr === -1) return safeNotify_('No encuentro id_address_delivery en Órdenes.');

  // 1) IDs únicos desde Órdenes
  const ids = shOrd.getRange(2, idxAddr+1, shOrd.getLastRow()-1, 1).getValues()
    .flat().filter(v => v !== '' && v !== null).map(String);
  const idsUnicos = [...new Set(ids)];
  if (!idsUnicos.length) return safeNotify_('No hay id_address_delivery en Órdenes.');

  // 2) Traer addresses desde API
  const lista = fetchByIds_({ recurso: 'addresses', fields: FIELDS.addresses, ids: idsUnicos, claveLista: 'addresses' });

  // 3) Esquema objetivo
  const headers = ['id','id_customer','alias','address1','address2','city','postcode','id_country','phone','phone_mobile'];

  // 4) Preparar hoja Direcciones y headers
  const shDir = ensureSheet_('Direcciones');
  let headersActuales = shDir.getRange(1,1,1,shDir.getLastColumn()).getValues()[0].map(h => String(h).trim())
    ;

  // Si no hay encabezados, los creamos
  if (headersActuales.length === 0) {
    shDir.appendRow(headers);
    headersActuales = headers.slice();
  } else {
    // Agrega al final cualquier columna faltante
    const missing = headers.filter(h => !headersActuales.includes(h));
    if (missing.length) {
      let last = shDir.getLastColumn();
      missing.forEach(h => {
        shDir.insertColumnAfter(last);
        shDir.getRange(1, last + 1).setValue(h);
        last++;
      });
      headersActuales = shDir.getRange(1,1,1,shDir.getLastColumn()).getValues()[0];
    }
  }

  const idIndex = headersActuales.indexOf('id');
  if (idIndex === -1) return safeNotify_('No encuentro columna id en Direcciones.');

  // 5) Índices y snapshot actual (para upsert idempotente)
  const lastRow = shDir.getLastRow();
  const nRows   = Math.max(0, lastRow - 1);
  const idsExistentes = nRows > 0
    ? shDir.getRange(2, idIndex+1, nRows, 1).getValues().flat().map(v => String(v).trim())
    : [];

  // Map de id → fila (base 2 por headers)
  const rowById = new Map();
  for (let i = 0; i < idsExistentes.length; i++) {
    if (idsExistentes[i]) rowById.set(idsExistentes[i], i + 2);
  }

  // Para lecturas idempotentes por campo, preparamos un snapshot de la tabla completa (si hay filas)
  let tableValues = null;
  if (nRows > 0) {
    tableValues = shDir.getRange(2, 1, nRows, headersActuales.length).getValues();
  }

  // 6) Construcción de filas + upsert idempotente
  let nuevas = 0, actualizadas = 0;

  // Helper local para obtener valor actual por columna
  const getCurrent = (rowIdx, colName) => {
    if (!tableValues || rowIdx < 2) return ''; // fila inexistente
    const colIndex = headersActuales.indexOf(colName);
    if (colIndex === -1) return '';
    const arrIdx = rowIdx - 2;
    if (arrIdx < 0 || arrIdx >= tableValues.length) return '';
    return tableValues[arrIdx][colIndex];
  };

  // Procesar cada address devuelta por la API
  for (const a of lista) {
    const id = String(a.id || '').trim();
    if (!id) continue;

    // Derivados
    const a1 = (a.address1 || '').trim();
    const a2 = (a.address2 || '').trim();
    const phone  = (a.phone || '').toString().trim();
    const mobile = (a.phone_mobile || '').toString().trim();

    const existsRow = rowById.get(id) || 0; // 0 si no existe

    if (!existsRow) {
      // 6.1) Nueva fila: construimos en el orden de 'headersActuales'
      const newRow = headersActuales.map(h => {
        switch (h) {
          case 'id':                  return id;
          case 'id_customer':         return a.id_customer || '';
          case 'alias':               return a.alias || '';
          case 'address1':            return a1;
          case 'address2':            return a2;
          case 'city':                return a.city || '';
          case 'postcode':            return a.postcode || '';
          case 'id_country':          return a.id_country || '';
          case 'phone':               return phone || '';
          case 'phone_mobile':        return mobile || '';
          default:                    return '';
        }
      });
      shDir.appendRow(newRow);
      nuevas++;
    } else {
      // 6.2) Upsert idempotente: NO sobrescribir con vacío.
      // Usar chooseValue_ para cada campo; derivar telefono_preferido.
             let changed = false;
      const outRow = [];

      // Recorremos columnas en el orden real de la hoja
      for (let c = 0; c < headersActuales.length; c++) {
        const colName = headersActuales[c];
        const current = getCurrent(existsRow, colName);
        let incoming = current; // default: conservar

        switch (colName) {
          case 'id':              incoming = id; break;
          case 'id_customer':     incoming = a.id_customer || ''; break;
          case 'alias':           incoming = a.alias || ''; break;
          case 'address1':        incoming = a1; break;
          case 'address2':        incoming = a2; break;
          case 'city':            incoming = a.city || ''; break;
          case 'postcode':        incoming = a.postcode || ''; break;
          case 'id_country':      incoming = a.id_country || ''; break;
          case 'phone':           incoming = phone; break;
          case 'phone_mobile':    incoming = mobile; break;
          default:
            incoming = current;
        }

        const chosen = chooseValue_(current, incoming); // CHANGE: idempotencia genérica
        outRow.push(chosen);
        if (String(chosen) !== String(current)) changed = true;
      }

      if (changed) {
        shDir.getRange(existsRow, 1, 1, outRow.length).setValues([outRow]);
        // Mantener snapshot coherente si volvemos a leer en esta pasada
        if (tableValues) {
          tableValues[existsRow - 2] = outRow.slice();
        }
        actualizadas++;
      }
    }
  }

  // 7) Notificación final
  const total = nuevas + actualizadas;
  safeNotify_(`Direcciones: ${total} procesadas (${nuevas} nuevas, ${actualizadas} actualizadas).`);
}

// Debug
function debugDireccionPorId_(addrId) {
  const sh = ensureSheet_('Direcciones');
  const headers = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  const idxId = headers.indexOf('id');
  const n = sh.getLastRow() - 1;
  const ids = sh.getRange(2, idxId+1, n, 1).getValues().flat().map(String);

  const row = ids.indexOf(String(addrId));
  if (row === -1) return Logger.log('No está ese id en hoja');

  // Vuelve a pedir a la API sólo ese address
  const [a] = fetchByIds_({
    recurso: 'addresses',
    fields: FIELDS.addresses,
    ids: [String(addrId)],
    claveLista: 'addresses'
  });

  const a1 = (a.address1||'').trim();
  const a2 = (a.address2||'').trim();
  const full = (a1 + (a2 ? ' '+a2 : '')).trim();
  const phone  = (a.phone||'').toString().trim();
  const mobile = (a.phone_mobile||'').toString().trim();
  const prefer = mobile || phone;

  Logger.log({curFull, full, curPref, prefer, a1, a2, phone, mobile});
}

function callDebug() {
  debugDireccionPorId_(2679)
}
