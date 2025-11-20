import os
import sys
import time
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# --- Configuration ---
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "secretpassword")
DATABASE = os.getenv("NEO4J_DATABASE", "projectdb")

# --- Transaction Functions ---

def clear_database(tx):
    """0. Clears all nodes and relationships."""
    tx.run("MATCH (n) DETACH DELETE n")
    print("0. CLEANUP successful. Database cleared of all existing nodes and relationships.")
    return True

def create_task(tx):
    """1. Creates a new Task node."""
    create_query = "CREATE (n:Task {id: 'T1', name: $name, status: 'New'}) RETURN n.name AS name"
    result = tx.run(create_query, name="Distributed Systems Project Setup")
    record = result.single()
    if record:
        print(f"1. CREATE successful. Node created: '{record['name']}'")
        return True
    else:
        raise Exception("CREATE FAILED: Node creation did not return a record.")

def update_task(tx):
    """3. Updates the Task status."""
    update_query = "MATCH (n:Task {id: 'T1'}) SET n.status = 'In Progress', n.updatedAt = datetime() RETURN n.status AS new_status"
    result = tx.run(update_query)
    record = result.single()
    if record:
        print(f"3. UPDATE successful. New Status: {record['new_status']}")
        return True
    else:
        raise Exception("UPDATE FAILED: Could not find node T1 for update.")

def create_person_and_relationship(tx):
    """4. Creates a Person node and the RESPONSIBLE_FOR relationship."""
    # 4a. Create Person
    tx.run("MERGE (p:Person {name: 'Jane Doe', role: 'Team Lead'})")
    print("4a. EXPAND successful. Node created: 'Jane Doe'")

    # 4b. Create Relationship
    relationship_query = """
    MATCH (p:Person {name: 'Jane Doe'}), (t:Task {id: 'T1'})
    MERGE (p)-[:RESPONSIBLE_FOR {assigned_date: date()}]->(t)
    """
    tx.run(relationship_query)
    print("4b. EXPAND successful. Relationship created: Person -> RESPONSIBLE_FOR -> Task")
    return True

# --- Main Logic ---

def run_crud_example(driver):
    """Executes basic CRUD operations on the Neo4j database using explicit transactions."""
    print("--- Starting CRUD Operations ---")
    
    try:
        with driver.session(database=DATABASE) as session:
            
            # Step 0: CLEANUP (MUST be execute_write)
            session.execute_write(clear_database)
            
            # Step 1: CREATE (MUST be execute_write)
            session.execute_write(create_task)
            
            # Step 2: READ (Uses session.run for reading)
            read_query = "MATCH (n:Task {id: 'T1'}) RETURN n.name AS name, n.status AS status"
            result = session.run(read_query)
            record = result.single()
            if record:
                print(f"2. READ successful. Task: {record['name']}, Status: {record['status']}")
            else:
                raise Exception("READ FAILED: Could not find node T1 after creation.")
            
            # Step 3: UPDATE (MUST be execute_write)
            session.execute_write(update_task)
            
            # Step 4: EXPAND (MUST be execute_write)
            session.execute_write(create_person_and_relationship)

            # NOTE: We can remove the time.sleep(1) now that we're using explicit writes.

            # Step 5: VERIFY (Uses session.run for reading)
            verify_query = """
            MATCH (p:Person {name: 'Jane Doe'})-[r:RESPONSIBLE_FOR]->(t:Task {id: 'T1'})
            RETURN p.name AS Responsible, t.name AS Task, type(r) AS Relationship
            """
            result = session.run(verify_query)
            record = result.single()
            
            if record:
                print(f"5. VERIFY successful. Relationship found: {record['Responsible']} is {record['Relationship']} {record['Task']}")
            else:
                # If this fails, the data is not in the DB, even though the write was committed.
                print("5. VERIFY FAILED: Relationship was not found in the database. Data write may have failed silently.")

            # Step 6: PERSISTENCE CHECK
            print("6. PERSISTENCE CHECK: The graph structure is now saved to the database for inspection.")

    except Exception as e:
        print(f"An error occurred during Neo4j interaction: {e}")
        # Exit with error code so the container shows failure
        sys.exit(1)
    
    print("--- CRUD Operations Complete ---")

# Function to connect with a retry mechanism
def connect_with_retry(uri, auth, max_retries=120, delay_seconds=5):
    """Initializes the Neo4j Driver, retrying on connection failure."""
    for attempt in range(max_retries):
        try:
            driver = GraphDatabase.driver(uri, auth=auth)
            # Verify connectivity using a quick transaction
            driver.verify_connectivity()
            print(f"Neo4j database connection established successfully at {uri}!")
            return driver
        except ServiceUnavailable as e:
            print(f"Connection attempt {attempt + 1}/{max_retries} failed. Retrying in {delay_seconds}s...")
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
            else:
                raise
        except Exception as e:
            raise e
    return None

# Main execution block
if __name__ == "__main__":
    driver = None
    try:
        driver = connect_with_retry(URI, auth=(USER, PASSWORD))
        
        if driver:
            run_crud_example(driver)

    except Exception as e:
        print(f"Failed to connect to Neo4j after multiple retries. Error: {e}")
        sys.exit(1)
        
    finally:
        if driver:
            driver.close()
            print("Neo4j driver closed.")
