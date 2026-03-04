import pandas as pd
import duckdb
import os
from rdflib import Graph, RDF, RDFS, OWL
from collections import defaultdict


# ---------------------------------------------------
# USER CONFIGURATION
# ---------------------------------------------------

KG_Matrix = "KGM/KGM.xlsx"
DuckDB = "ontop.duckdb"
output_dir = "owl"
data_structure = "raw/*/*/*.csv"
data_format = "csv"  # options: csv,json
ontology_path = "HPCC.ttl"


# ---------------------------------------------------
# UTILITIES
# ---------------------------------------------------

sheets = pd.read_excel(KG_Matrix, sheet_name=None)

def explode_multivalue_columns(df, delimiter=","):
    df = df.copy()
    # Identify columns with comma-separated values
    multi_value_cols = [
        col for col in df.columns
        if df[col].dropna().astype(str).str.contains(delimiter).any()
    ]
    
    for col in multi_value_cols:
        df[col] = df[col].astype(str).str.split(delimiter)
    
    if multi_value_cols:
        df = df.explode(multi_value_cols).reset_index(drop=True)
        # Trim whitespace
        for col in multi_value_cols:
            df[col] = df[col].str.strip()
    
    return df


# ---------------------------------------------------
# EXPORT SHEETS
# ---------------------------------------------------

print("\n🔁 Exporting sheets to CSV...")

for sheet_name, df in sheets.items():
    view_name = sheet_name.strip().lower().replace(" ", "_")
    csv_path = os.path.join(output_dir, f"{sheet_name}.csv")
    
    # Normalize: explode multi-value columns into rows
    df_clean = explode_multivalue_columns(df)
    
    # Save normalized CSV
    df_clean.to_csv(csv_path, index=False, sep=';')
    print(f"Sheet '{sheet_name}' exported to '{csv_path}'")


# ---------------------------------------------------
# ONTOLOGY VALIDATION
# ---------------------------------------------------

# Load all sheets
sheets_dict = pd.read_excel(KG_Matrix, sheet_name=None)

# Build class → individuals mapping
class_to_individuals = {}
for class_name, df in sheets_dict.items():
    if "Individual" in df.columns:
        individuals = [ind.strip() for ind in df["Individual"].dropna().astype(str).tolist()]
        class_to_individuals[class_name] = individuals

# Load ontology
g = Graph()
g.parse(ontology_path, format="turtle")

# Object Property → Range Class mapping
object_property_ranges = {}
for s in g.subjects(RDF.type, OWL.ObjectProperty):
    range_class = g.value(s, RDFS.range)
    if range_class:
        prop_name = g.namespace_manager.normalizeUri(s)
        range_name = g.namespace_manager.normalizeUri(range_class)
        object_property_ranges[prop_name] = range_name

# Build class → all subclasses (recursively)
class_subclasses = defaultdict(set)
for subclass, _, superclass in g.triples((None, RDFS.subClassOf, None)):
    superclass_name = g.namespace_manager.normalizeUri(superclass)
    subclass_name = g.namespace_manager.normalizeUri(subclass)
    class_subclasses[superclass_name].add(subclass_name)

def get_all_subclasses(cls):
    subclasses = set()
    for sub in class_subclasses.get(cls, []):
        subclasses.add(sub)
        subclasses.update(get_all_subclasses(sub))
    return subclasses

print("\n🔍 Validating Property Matrix:\n")

for class_name, df in sheets_dict.items():
    if "Individual" not in df.columns:
        continue

    sheet_errors = []

    for col in df.columns[2:]:
        if col not in object_property_ranges:
            continue

        expected_range_class = object_property_ranges[col]
        range_classes_to_check = {expected_range_class}
        range_classes_to_check.update(get_all_subclasses(expected_range_class))

        valid_individuals = []
        for rc in range_classes_to_check:
            local_name = rc.split(":")[-1]
            valid_individuals.extend(class_to_individuals.get(local_name, []))

        for i, cell in df[col].dropna().items():
            subject = df.at[i, "Individual"].strip()
            values = [v.strip() for v in str(cell).split(",") if v.strip()]
            for value in values:
                if value not in valid_individuals:
                    sheet_errors.append(
                        f"⚠️  [{class_name}] {subject} → {col} → '{value}' is NOT an individual of expected class '{expected_range_class}' (or subclasses)"
                    )

    if sheet_errors:
        print(f"📄 Sheet: {class_name} ❌")
        for err in sheet_errors:
            print("  ", err)
    else:
        print(f"📄 Sheet: {class_name} ✅")


# ---------------------------------------------------
# CREATE VIEWS
# ---------------------------------------------------

con = duckdb.connect("ontop.duckdb")


# Create subclasses view 
con.execute("""
CREATE OR REPLACE VIEW subclasses AS
SELECT
  "Class" AS Class,
  "rdfs:subClassOf" AS rdfs__subClassOf
FROM read_csv_auto(
  'owl/Subclass.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")


print("\n✅ subclasses view created:")

result = con.execute("SELECT * FROM subclasses LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))

# Create  observation view
if data_format == "csv":
    reader = f"read_csv_auto('{data_structure}', AUTO_DETECT=TRUE, FILENAME=TRUE)"
elif data_format == "json":
    reader = f"read_json_auto('{data_structure}', FILENAME=TRUE)"

con.execute(f"""
CREATE OR REPLACE VIEW observation AS
SELECT
    regexp_replace(
        split_part(REPLACE(filename, '\\', '/'), '/', -1),
        '\\.(csv|json)$',
        ''
    ) AS sensor,
    timestamp,
    value
FROM {reader};
""")


print("\n✅ observation view created.")
result = con.execute("SELECT * FROM observation LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))

# Create platform view 
con.execute("""
CREATE OR REPLACE VIEW platform AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "sosa:hosts" AS sosa__hosts
FROM read_csv_auto(
  'owl/Platform.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ platform view created:")

result = con.execute("SELECT * FROM platform LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create generalsensor view 
con.execute("""
CREATE OR REPLACE VIEW generalsensor AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:hasTargetValue" AS hpcc__hasTargetValue,
  "sosa:isHostedBy" AS sosa__isHostedBy,
  "sosa:madeObservation" AS sosa__madeObservation,
  "sosa:observes" AS sosa__observes,
  "ssn:detects" AS ssn__detects,

FROM read_csv_auto(
  'owl/GeneralSensor.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ generalsensor view created:")

result = con.execute("SELECT * FROM generalsensor LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create recoolersensor view 
con.execute("""
CREATE OR REPLACE VIEW recoolersensor AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:hasTargetValue" AS hpcc__hasTargetValue,
  "sosa:isHostedBy" AS sosa__isHostedBy,
  "sosa:madeObservation" AS sosa__madeObservation,
  "sosa:observes" AS sosa__observes,
  "ssn:detects" AS ssn__detects,

FROM read_csv_auto(
  'owl/RecoolerSensor.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ recoolersensor view created:")

result = con.execute("SELECT * FROM recoolersensor LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create freecoolersensor view 
con.execute("""
CREATE OR REPLACE VIEW freecoolersensor AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:hasTargetValue" AS hpcc__hasTargetValue,
  "sosa:isHostedBy" AS sosa__isHostedBy,
  "sosa:madeObservation" AS sosa__madeObservation,
  "sosa:observes" AS sosa__observes,
  "ssn:detects" AS ssn__detects,

FROM read_csv_auto(
  'owl/FreeCoolerSensor.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ freecoolersensor view created:")

result = con.execute("SELECT * FROM freecoolersensor LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create meanobservation view 
con.execute("""
CREATE OR REPLACE VIEW meanobservation AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "qudt:hasUnit" AS qudt__hasUnit,
  "sosa:hasFeatureOfInterest" AS sosa__hasFeatureOfInterest,
  "sosa:hasSimpleResult" AS sosa__hasSimpleResult,
  "sosa:madeBySensor" AS sosa__madeBySensor,
  "sosa:observedProperty" AS sosa__observedProperty,

FROM read_csv_auto(
  'owl/MeanObservation.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ meanobservation view created:")

result = con.execute("SELECT * FROM meanobservation LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create featureofinterest view 
con.execute("""
CREATE OR REPLACE VIEW featureofinterest AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "sosa:isFeatureOfInterestOf" AS sosa__isFeatureOfInterestOf
FROM read_csv_auto(
  'owl/FeatureOfInterest.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ featureofinterest view created:")

result = con.execute("SELECT * FROM featureofinterest LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create heatpumpsensor view 
con.execute("""
CREATE OR REPLACE VIEW heatpumpsensor AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:hasTargetValue" AS hpcc__hasTargetValue,
  "sosa:isHostedBy" AS sosa__isHostedBy,
  "sosa:madeObservation" AS sosa__madeObservation,
  "sosa:observes" AS sosa__observes,
  "ssn:detects" AS ssn__detects,

FROM read_csv_auto(
  'owl/HeatPumpSensor.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ heatpumpsensor view created:")

result = con.execute("SELECT * FROM heatpumpsensor LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create heatexchangersensor view 
con.execute("""
CREATE OR REPLACE VIEW heatexchangersensor AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:hasTargetValue" AS hpcc__hasTargetValue,
  "sosa:isHostedBy" AS sosa__isHostedBy,
  "sosa:madeObservation" AS sosa__madeObservation,
  "sosa:observes" AS sosa__observes,
  "ssn:detects" AS ssn__detects,

FROM read_csv_auto(
  'owl/HeatExchangerSensor.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ heatexchangersensor view created:")

result = con.execute("SELECT * FROM heatexchangersensor LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create pumpsensor view 
con.execute("""
CREATE OR REPLACE VIEW pumpsensor AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:hasTargetValue" AS hpcc__hasTargetValue,
  "sosa:isHostedBy" AS sosa__isHostedBy,
  "sosa:madeObservation" AS sosa__madeObservation,
  "sosa:observes" AS sosa__observes,
  "ssn:detects" AS ssn__detects,

FROM read_csv_auto(
  'owl/PumpSensor.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ pumpsensor view created:")

result = con.execute("SELECT * FROM pumpsensor LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create targetvalue view 
con.execute("""
CREATE OR REPLACE VIEW targetvalue AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:setForSensor" AS hpcc__setForSensor,
  "qudt:hasUnit" AS qudt__hasUnit,
  "qudt:numericValue" AS qudt__numericValue
FROM read_csv_auto(
  'owl/TargetValue.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ targetvalue view created:")

result = con.execute("SELECT * FROM targetvalue LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create coolersensor view 
con.execute("""
CREATE OR REPLACE VIEW coolersensor AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "hpcc:hasTargetValue" AS hpcc__hasTargetValue,
  "sosa:isHostedBy" AS sosa__isHostedBy,
  "sosa:madeObservation" AS sosa__madeObservation,
  "sosa:observes" AS sosa__observes,
  "ssn:detects" AS ssn__detects,

FROM read_csv_auto(
  'owl/CoolerSensor.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ coolersensor view created:")

result = con.execute("SELECT * FROM coolersensor LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


# Create observableproperty view 
con.execute("""
CREATE OR REPLACE VIEW observableproperty AS
SELECT
  "Individual" AS Individual,
  "rdf:type" AS rdf__type,
  "sosa:isObservedBy" AS sosa__isObservedBy
FROM read_csv_auto(
  'owl/ObservableProperty.csv',
  DELIM=';',
  HEADER=TRUE,
  AUTO_DETECT=TRUE
)
""")

print("\n✅ observableproperty view created:")

result = con.execute("SELECT * FROM observableproperty LIMIT 10")
rows = result.fetchall()
cols = [desc[0] for desc in result.description]
widths = [max(len(str(col)), max(len(str(row[i])) for row in rows)) for i, col in enumerate(cols)]
print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
for row in rows: print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))

