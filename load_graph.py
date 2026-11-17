import pandas as pd
from neo4j import GraphDatabase

def load_batch(tx, batch_records):
  """Cypher query using UNWIND to batch-load nodes and relationships

  according to the exact schema specification (without uniqueness constraints).
  """
  query = """
    UNWIND $batch AS row
    
    MERGE (d:Driver {driver_id: row.driver_id})
    MERGE (c:Company {name: row.company})
    MERGE (a:Area {area_id: row.dropoff_area})
    
    MERGE (d)-[:WORKS_FOR]->(c)
    MERGE (d)-[:TRIP {trip_id: row.trip_id, fare: row.fare, trip_seconds: row.trip_seconds}]->(a)
    """
  tx.run(query, batch=batch_records)

def main():
	csv_file = "taxi_trips_clean.csv"
	print(f"Reading data from {csv_file}...")
	df = pd.read_csv(csv_file)

	driver = GraphDatabase.driver("bolt://34.28.51.137:7687", auth=("neo4j", "498dmc1613"))

	with driver.session() as session:
		records = df.to_dict(orient="records")
		batch_size = 1000
		total_records = len(records)
		for i in range(0, total_records, batch_size):
			batch = records[i : i + batch_size]
			session.execute_write(load_batch, batch)

	driver.close()
	print("Graph loading complete.")

if __name__ == "__main__":
  main()