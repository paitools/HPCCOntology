# HPCCool - A Semantic Framework for HPC Cooling Management and Advanced Analytics
![Contributions](https://img.shields.io/badge/Format-RDF/XML-blue)
![Contributions](https://img.shields.io/badge/Format-TTL-blue)
![Contributions](https://img.shields.io/badge/Language-Python-blue)

HPCCool is a semantic integration framework that bridges ontological system representations with raw sensory data in HPC cooling environments. At its core, the framework incorporates the HPCC ontology, which formally models HPC cooling management systems, including thermal exchange units, cooling devices, valves, and pumps, as observed through a distributed sensor network. HPCCool builds upon an ontology-based data access (OBDA) engine (Ontop), enabling R2RML mappings between ontological entities and relational data sources.

## HPCC Ontology:

<p align="center">
  <img src="docs/resources//HPCC_Ontology_v2_crop.png" width="90%" alt="HPCC Ontology">
  <br><em>HPCC Ontology: The core concepts and semantic relationships</em>
</p>

The HPCC Ontology provides a semantic model for representing HPC thermal management telemetry. It extends the W3C SSN/SOSA standard to capture HPC cooling concepts, including heat pumps, heat exchangers, pumps, coolers, and re-coolers. Designed for the datacenter thermal management domain, the ontology supports anomaly detection, real-time condition monitoring, and advanced system analytics.


## Ontology Documentation:

Ontology Specification with permanent `w3id.org/hpcc` identifier:

[![Documentation](https://img.shields.io/badge/Documentation-HPCC_Ontology-blue)](https://paitools.github.io/HPCCOntology/docs/index-en.html)

## HPCCool User Guide

Running **HPCCool** on user hardware involves two steps:

1. **Populate the Knowledge Graph Matrix (KGM)**
2. **Deploy the Framework**


### 1. KGM Population

1. Open the `KGM_Template.xlsx` within the `KGM` folder.
   
2. Populate the template with project-specific instances. If needed, use the provided KGM as a reference.

3. Rename `KGM_Template.xlsx` to `KGM.xlsx` and store it in the same folder.

   


### 2. HPCCool Deployment

- After completing the KGM, set the `data_structure` to your HPC thermal logging structure (e.g., `raw/*/*/*.csv`).
  
- Deploy the framework:
  ```bash
  python3 HPCCool.py

### Running SPARQL

- To run SPARQL queries (e.g., `user_query.rq`) on real-time data:

   ```bash
   ontop.bat query -p ontop.properties -m mapping.ttl -q user_query.rq

Requirements

- DuckDB ≥ `1.0.0`
- Ontop client ≥ `5.3.0`

Ensure both are installed before running the framework. 
If compatibility issues occur, use the exact versions listed above.



## License

All resources are licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International](https://creativecommons.org/licenses/by-nc-sa/4.0/) license.

![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)


## Citation

Ivanovic, P., & Wanniarachchi, S. (2026). paitools/HPCCOntology: HPCCool/HPCC v1.0: Pre-publication Release. Zenodo. https://doi.org/10.5281/zenodo.19947986
