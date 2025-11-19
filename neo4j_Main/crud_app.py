import os
import sys
from neo4j import GraphDatabase, AsyncGraphDatabase

# --- Configuration ---
# Uses environment variables set in docker-compose.yml
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "secretpassword")
DATABASE = os.getenv("NEO4J_DATABASE", "finalProject")

# A simple CRUD transaction function
def run_crud_example(driver):
    """Executes basic CRUD operations on the Neo4j database."""
    print("--- Starting CRUD Operations ---")
    
    try:
        # Create a simple session
        with driver.session(database=DATABASE) as session:
            
            # 1. CREATE Operation: Create a new node
            create_query = "CREATE (n:Task {id: 'T1', name: $name, status: 'New'}) RETURN n.name AS name"
            result = session.run(create_query, name="Distributed Systems Project Setup")
            message = result.single()["name"]
            print(f"1. CREATE successful. Node created: '{message}'")
            
            # 2. READ Operation: Read the node back
            read_query = "MATCH (n:Task {id: 'T1'}) RETURN n.name AS name, n.status AS status"
            result = session.run(read_query)
            record = result.single()
            print(f"2. READ successful. Task: {record['name']}, Status: {record['status']}")
            
            # 3. UPDATE Operation: Change the status
            update_query = "MATCH (n:Task {id: 'T1'}) SET n.status = 'In Progress', n.updatedAt = datetime() RETURN n.status AS new_status"
            result = session.run(update_query)
            new_status = result.single()["new_status"]
            print(f"3. UPDATE successful. New Status: {new_status}")
            
            # 4. DELETE Operation: Clean up the node
            # delete_query = "MATCH (n:Task {id: 'T1'}) DELETE n"
            # session.run(delete_query)
            # print("4. DELETE successful. Task node cleaned up.")
            print("4. DELETE Operation SKIPPED. The Task node is now saved to the database for inspection.")

    except Exception as e:
        print(f"An error occurred during Neo4j interaction: {e}")
        # Exit with error code so the container shows failure
        sys.exit(1)
    
    print("--- CRUD Operations Complete ---")


# Function to connect with a retry mechanism
def connect_with_retry(uri, auth, max_retries=60, delay_seconds=5):
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
            print(f"Error detail: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
            else:
                raise # Re-raise the exception if all retries fail
        except Exception as e:
            # Handle other driver-related errors (like auth errors, network errors)
            raise e
    return None



# Main execution block
if __name__ == "__main__":
    driver = None
    try:
        # Initialize the Neo4j Driver
        driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
        
        # Verify connectivity using a quick transaction
        driver.verify_connectivity()
        print(f"Neo4j database connection established successfully at {URI}!")
        
        # Run the CRUD example
        run_crud_example(driver)

    except Exception as e:
        print(f"Failed to connect to Neo4j. Please ensure the 'neo4j-db' service is running and healthy. Error: {e}")
        sys.exit(1)
        
    finally:
        # Close the driver connection
        if driver:
            driver.close()
            print("Neo4j driver closed.")
