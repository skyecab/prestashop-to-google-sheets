function traerDirecciones() {
  const shOrd = ensureSheet_('Órdenes');
  if (shOrd.getLastRow() < 2) return safeNotify_('Órdenes está vacío.');

  const headersOrd = shOrd.getRange(1,1,1,shOrd.getLastColumn()).getValues()[0];
  const idxAddr = headersOrd.indexOf('id_address_delivery');
  if (idxAddr === -1) return safeNotify_('No encuentro id_address_delivery en Órdenes.');

  const ids = shOrd.getRange(2, idxAddr+1, shOrd.getLastRow()-1, 1).getValues()
    .flat().filter(v => v !== '' && v !== null).map(String);
  const idsUnicos = [...new Set(ids)];
  if (!idsUnicos.length) return safeNotify_('No hay id_address_delivery en Órdenes.');

  const lista = fetchByIds_({ recurso: 'addresses', fields: FIELDS.addresses, ids: idsUnicos, claveLista: 'addresses' });

  const headers = ['id','id_customer','alias','address1','address2','direccion_completa','city','postcode','id_country','phone','phone_mobile','telefono_preferido'];
  const rows = lista.map(a => {
    const a1 = a.address1 || '', a2 = a.address2 || '';
    const dirCompleta = (a1 + (a2 ? (' ' + a2) : '')).trim();
    const telPreferido = (a.phone_mobile && String(a.phone_mobile).trim()) ? a.phone_mobile : (a.phone || '');
    return [a.id||'', a.id_customer||'', a.alias||'', a1, a2, dirCompleta, a.city||'', a.postcode||'', a.id_country||'', a.phone||'', a.phone_mobile||'', telPreferido];
  });

  const shDir = ensureSheet_('Direcciones');
  let headersActuales = shDir.getLastRow() > 0 
    ? shDir.getRange(1,1,1,shDir.getLastColumn()).getValues()[0]
    : [];

  // Si no hay encabezados, los creamos
  if (headersActuales.length === 0) {
    shDir.appendRow(headers);
    headersActuales = headers;
  }

  const idIndex = headersActuales.indexOf('id');
  if (idIndex === -1) return safeNotify_('No encuentro columna id en Direcciones.');

  const idsExistentes = shDir.getRange(2, idIndex+1, shDir.getLastRow()-1, 1)
    .getValues().flat().map(String);

  let filasAgregadas = 0;
  rows.forEach(row => {
    const id = String(row[0]);
    const idxExistente = idsExistentes.indexOf(id);
    if (idxExistente === -1) {
      shDir.appendRow(row);
      filasAgregadas++;
    } else {
      shDir.getRange(idxExistente + 2, 1, 1, row.length).setValues([row]);
    }
  });

  safeNotify_(`Direcciones: ${rows.length} procesadas (${filasAgregadas} nuevas, ${rows.length - filasAgregadas} actualizadas).`);
}
