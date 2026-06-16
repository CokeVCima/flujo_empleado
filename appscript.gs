function doPost(e) {
  try {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    
    var datosRecibidos = JSON.parse(e.postData.contents);
    var filasParaInsertar = datosRecibidos.filas; 
    
    if (filasParaInsertar && filasParaInsertar.length > 0) {
      
      sheet.clear();
      
      var filasActuales = sheet.getMaxRows();
      if (filasActuales > 1) {
        sheet.deleteRows(1, filasActuales - 1);
      }
      
      var numFilas = filasParaInsertar.length;
      var numColumnas = filasParaInsertar[0].length;
      var rangoTotal = sheet.getRange(1, 1, numFilas, numColumnas);
      
      rangoTotal.setValues(filasParaInsertar);
      
      aplicarFormatoEjecutivo(sheet, numFilas, numColumnas);
    }
    
    return ContentService.createTextOutput(JSON.stringify({"status": "success", "message": filasParaInsertar.length + " filas insertadas y formateadas"}))
                         .setMimeType(ContentService.MimeType.JSON);
                         
  } catch(error) {
    return ContentService.createTextOutput(JSON.stringify({"status": "error", "message": error.toString()}))
                         .setMimeType(ContentService.MimeType.JSON);
  }
}


function aplicarFormatoEjecutivo(sheet, filas, columnas) {
  var rangoTotal = sheet.getRange(1, 1, filas, columnas);
  
  rangoTotal.setFontFamily("Arial")
            .setFontSize(10)
            .setVerticalAlignment("middle");
  
  var rangoCabecera = sheet.getRange(1, 1, 1, columnas);
  rangoCabecera.setBackground("#1F4E79") // Azul oscuro
               .setFontColor("#FFFFFF")  // Texto blanco
               .setFontWeight("bold")    // Negrita
               .setHorizontalAlignment("center") 
               .setFontSize(11);         
  
  if (filas > 1) {
    var rangoCuerpo = sheet.getRange(2, 1, filas - 1, columnas);
    rangoCuerpo.setHorizontalAlignment("left"); // Por defecto todo a la izquierda
    
    sheet.getRange(2, 1, filas - 1, 1).setHorizontalAlignment("center");  // NumeroEmpleado
    sheet.getRange(2, 5, filas - 1, 2).setHorizontalAlignment("center");  // Fechas de ingreso
    sheet.getRange(2, 7, filas - 1, 1).setHorizontalAlignment("center");  // ActivoRH
    sheet.getRange(2, 9, filas - 1, 1).setHorizontalAlignment("center");  // FechaNacimiento
  }
  
  rangoTotal.setBorder(true, true, true, true, true, true, "#D3D3D3", SpreadsheetApp.BorderStyle.SOLID);
  
  for (var col = 1; col <= columnas; col++) {
    sheet.autoResizeColumn(col);
    var anchoActual = sheet.getColumnWidth(col);
    sheet.setColumnWidth(col, anchoActual + 20);
  }
  
  sheet.setFrozenRows(1);
}
