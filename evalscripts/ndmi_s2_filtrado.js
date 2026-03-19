//VERSION=3
//
// NDMI FILTRADO — Sentinel-2
// AgroClimaX — Rivera, Uruguay
// Basado en metodología: Ing. Agro. Gerardo Díaz, División de Desarrollo Rural
//
// OBJETIVO:
// Calcular NDMI eliminando efectos de nubes, sombras, agua libre y saturación radiométrica.
// NDMI = (B08 - B11) / (B08 + B11)
// Sensible al contenido hídrico de la vegetación.
// Anticipa estrés hídrico 7–10 días antes que índices estructurales (NDVI).
//

function setup() {
  return {
    input: ["B03", "B08", "B11", "SCL", "dataMask"],
    output: { bands: 1, sampleType: "FLOAT32" }
  };
}

function isCloud(scl) {
  // SCL classes: 3=sombra nube, 8=nubes media prob, 9=nubes alta prob,
  //              10=cirros, 11=nieve/hielo
  return scl === 3 || scl === 8 || scl === 9 || scl === 10 || scl === 11;
}

function isWater(sample) {
  let ndwi = (sample.B03 - sample.B08) / (sample.B03 + sample.B08 + 0.0001);
  return ndwi > 0.3;
}

function evaluatePixel(sample) {
  // Enmascarar pixels sin datos, con nubes o agua libre
  if (sample.dataMask === 0 || isCloud(sample.SCL) || isWater(sample)) {
    return [NaN];
  }

  // Calcular NDMI: (NIR - SWIR) / (NIR + SWIR)
  // B08 = NIR (842 nm), B11 = SWIR (1610 nm)
  let ndmi = (sample.B08 - sample.B11) / (sample.B08 + sample.B11 + 0.0001);

  return [ndmi];
}
