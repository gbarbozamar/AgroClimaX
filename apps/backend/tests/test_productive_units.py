import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

import shapefile

from app.services.productive_units import parse_productive_units_file


class ProductiveUnitFileParsingTests(unittest.TestCase):
    def test_parse_geojson_upload(self):
        payload = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-56.0, -31.0], [-55.9, -31.0], [-55.9, -31.1], [-56.0, -31.1], [-56.0, -31.0]]],
                    },
                    "properties": {"name": "Predio Demo"},
                }
            ],
        }
        parsed = parse_productive_units_file("predios.geojson", json.dumps(payload).encode("utf-8"))
        self.assertEqual(parsed["type"], "FeatureCollection")
        self.assertEqual(len(parsed["features"]), 1)
        self.assertEqual(parsed["features"][0]["properties"]["name"], "Predio Demo")

    def test_parse_zip_shapefile_upload(self):
        with tempfile.TemporaryDirectory(prefix="agroclimax_test_shp_") as tmp_dir:
            shp_base = Path(tmp_dir) / "predios"
            writer = shapefile.Writer(str(shp_base))
            writer.field("name", "C")
            writer.poly([[[-56.0, -31.0], [-55.9, -31.0], [-55.9, -31.1], [-56.0, -31.1], [-56.0, -31.0]]])
            writer.record("Predio ZIP")
            writer.close()

            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for suffix in (".shp", ".shx", ".dbf"):
                    archive.write(shp_base.with_suffix(suffix), arcname=f"predios{suffix}")

            parsed = parse_productive_units_file("predios.zip", buffer.getvalue())

        self.assertEqual(parsed["type"], "FeatureCollection")
        self.assertEqual(len(parsed["features"]), 1)
        self.assertEqual(parsed["features"][0]["properties"]["name"].strip(), "Predio ZIP")


if __name__ == "__main__":
    unittest.main()
