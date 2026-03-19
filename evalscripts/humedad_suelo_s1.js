//VERSION=3
// ============================================================
// HUMEDAD DE SUELO SUPERFICIAL — Sentinel-1
// AgroClimaX — Rivera, Uruguay
// Basado en metodología: Ing. Agro. Gerardo Díaz, División de Desarrollo Rural
//
// PRINCIPIO FÍSICO:
// - El contenido de agua del suelo aumenta su constante dieléctrica
// - Esto incrementa la retrodispersión radar (σ0)
// - El NDMI (Sentinel-2) refleja el estado hídrico de la vegetación
// - Se establece una relación empírica entre VV(dB) y NDMI
//
// SALIDA — Raster monobanda UINT8:
//   0–253  → humedad del suelo (valor / 2.54 = % humedad)
//   254    → vegetación densa (NDVI > 0.7, suelo no visible por radar)
//   255    → agua libre
//
// LIMITACIONES:
// - Profundidad de penetración ~5 cm (banda C)
// - Más confiable con NDVI < 0.7
// - VH sensible a estructura vegetal, VV sensible a humedad suelo
// ============================================================

function setup() {
  return {
    input: ["VV", "VH", "dataMask"],
    output: { bands: 1, sampleType: "UINT8" }
  };
}

// ============================================================
// MATRIZ DE CALIBRACIÓN — Rivera, Uruguay
// Calibración: 01/03/2026 — Ing. Agro. Gerardo Díaz
// Formato: [VV_dB, NDMI]
// Gradiente: suelo muy seco → suelo muy húmedo
// ============================================================
let puntos_calibracion = [
  [-16.92, -0.33],  // P1 suelo muy seco  | 54°59'35.96"W  31°29'22.66"S
  [-13.49, -0.11],  // P2 suelo seco       | 54°59'19.29"W  31°29'21.06"S
  [-12.42,  0.07],  // P3 humedad media    | 54°59'26.68"W  31°30'01.28"S
  [-10.96,  0.25],  // P4 suelo húmedo     | 54°58'31.91"W  31°30'16.09"S
  [ -8.97,  0.44],  // P5 suelo muy húmedo | 55°00'16.73"W  31°30'09.53"S
];

// ============================================================
// INTERPOLACIÓN LINEAL: VV(dB) → NDMI estimado
// Extrapolación plana fuera del rango calibrado
// ============================================================
function vvANdmi(vv_dB) {
  let puntos = [...puntos_calibracion].sort((a, b) => a[0] - b[0]);
  let x = puntos.map(p => p[0]);
  let y = puntos.map(p => p[1]);

  // Asegurar monotonicidad (NDMI no decrece con VV creciente)
  for (let i = 1; i < y.length; i++) {
    if (y[i] < y[i - 1]) y[i] = y[i - 1];
  }

  // Extrapolación plana fuera del rango
  if (vv_dB <= x[0]) return y[0];
  if (vv_dB >= x[x.length - 1]) return y[y.length - 1];

  // Interpolación lineal entre puntos adyacentes
  for (let i = 0; i < x.length - 1; i++) {
    if (vv_dB >= x[i] && vv_dB <= x[i + 1]) {
      let t = (vv_dB - x[i]) / (x[i + 1] - x[i]);
      return y[i] + t * (y[i + 1] - y[i]);
    }
  }
  return y[0];
}

// ============================================================
// CONVERSIÓN NDMI → % HUMEDAD RELATIVA
// Rango representativo para suelos de Rivera
// ============================================================
function ndmiAHumedad(ndmi) {
  let ndmi_min = -0.5;
  let ndmi_max = 0.5;
  let humedad = 100 * (ndmi - ndmi_min) / (ndmi_max - ndmi_min);
  return Math.min(100, Math.max(0, humedad));
}

function evaluatePixel(sample) {
  let vv = sample.VV;
  let vh = sample.VH;
  let mask = sample.dataMask;

  if (!vv || !vh || vv <= 0 || vh <= 0 || !mask) return [0];

  // Conversión a decibelios
  let vv_dB = 10 * Math.log10(vv);
  let vh_dB = 10 * Math.log10(vh);

  // Detección de agua libre: alta retrodispersión especular
  // (agua libre → señal muy baja en SAR)
  if (vv_dB < -22) return [255];

  // Detección de vegetación densa usando ratio VH/VV
  // VH dominante respecto a VV indica dosel vegetal denso
  let ratio_vh_vv = vh_dB - vv_dB;
  if (ratio_vh_vv > -5) return [254];

  // Estimación de humedad mediante calibración multipunto
  let ndmi_estimado = vvANdmi(vv_dB);
  let humedad = ndmiAHumedad(ndmi_estimado);

  // Escalar a UINT8 (0–253): valor / 2.54 = % humedad
  let valor = Math.round(humedad * 2.54);
  return [Math.min(253, Math.max(0, valor))];
}
