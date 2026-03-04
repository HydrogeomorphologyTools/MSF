# MSF (Modified Single Flow) Debris Flow Hazard Assessment

![MSF Logo](assets/image.jpg)

**MSF (Modified Single Flow)** is a regional-scale GIS-based tool designed for the assessment of debris-flow runout zones and susceptibility. By leveraging Digital Elevation Models (DEMs) and initiation points, the software simulates the lateral spreading and deposition of mass movements with high computational efficiency.

---

## 🇪🇺 Funding & Acknowledgements
This project has been developed under the framework of:
*   **Next Generation EU** (PNRR)
*   **Ministero dell'Università e della Ricerca (MUR)**

We acknowledge the financial support which made this regional-scale analysis tool possible.

---

## 📚 Scientific Reference
The underlying algorithm is based on the Modified Single Flow model developed and popularized by:
1.  **Huggel, C., Kääb, A., Haeberli, W., & Krummenacher, B. (2003).** *"Regional-scale GIS-models for assessment of hazards from glacier lake outburst floods: combined determination of flood propagation and avalanche runout."* Natural Hazards and Earth System Sciences.
2.  **Gruber, S., Huggel, C., & Pike, R. J. (2009).** *"Modelling mass movements."* In Geomorphometry: Concepts, Software, Applications.

---

## 🚀 Key Features
*   **Regional Batch Processing:** Handle thousands of initiation points efficiently using multiprocessing.
*   **Flexible Inputs:** Supports both Shapefiles (point/polygon) and Raster data for initiation zones.
*   **Automated Pre-processing:** Integrated pit-filling and flow direction calculation via direct WhiteboxTools integration.
*   **Resampling Engine:** Built-in DTM resampling with median/mean/bilinear aggregation methods.
*   **Dual Interface:** Full Graphical User Interface (PyQt5) for interactive use and a Command Line Interface (CLI) for headless/automated workflows.
*   **Standalone Executable:** Fully portable version available in Releases (no Python installation required).

---

## 🛠 Usage

### Graphical User Interface (GUI)
Simply run the standalone executable or:
```bash
python main.py
```
The interface is organized into tabs:
1.  **Inputs:** Define your Base DTM, Initiation Points, and Output naming.
2.  **Processing:** Configure automated pit filling and flow direction calculation.
3.  **MSF Model:** Set scientific parameters ($H/L$ threshold, diversion angles, etc.).
4.  **Parallel:** Configure CPU workers for batch processing.

### Command Line Interface (CLI)
For automated tasks, use the CLI mode by passing arguments:
```bash
MSF_Regional_Unified.exe --config my_settings.json
```
To dump a template configuration:
```bash
MSF_Regional_Unified.exe --dump-config template.json
```

---

## ⚙️ Requirements (for Source)
If running from source, you need Python 3.10+ and the following libraries:
*   `rasterio`
*   `numpy`
*   `geopandas`
*   `PyQt5` (for GUI)
*   `whitebox` (Python wrapper for pre-compiled binary)

---

## 📦 Compilation
To build your own standalone executable:
1.  Place the `whitebox_tools.exe` binary in the `WBT/` folder.
2.  Run PyInstaller:
```bash
pyinstaller --clean msf_standalone.spec
```

---

## 📝 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 📬 Contact & Support
Developed for regional-scale hazard analysis. For bugs and feature requests, please open an Issue on this GitHub repository.
