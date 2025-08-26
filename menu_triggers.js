function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Pedidos')
    .addItem('Sincronizar todo ahora', 'actualizarOrdenes') // Órdenes + (Clientes/Direcciones automáticos si hay nuevas)
    .addSeparator()
    .addItem('Actualizar clientes (manual)', 'traerClientes')
    .addItem('Actualizar direcciones (manual)', 'traerDirecciones')
    .addSeparator()
    .addItem('Crear/renovar trigger horario', 'setupTriggers')
    .addToUi();
}

/** Correr manualmente para crear un trigger cada hora que ejecute actualizarOrdenes */
function setupTriggers() {
  // Limpia triggers previos para evitar duplicados
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'actualizarOrdenes')
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger('actualizarOrdenes')
    .timeBased()
    .everyHours(1)
    .create();

  Logger.log('Trigger creado: actualizarOrdenes cada hora.');
  safeNotify_('Trigger creado: actualizarOrdenes cada hora.');
}