# Distributed Data Storage System

## Overview

This project implements a small data storage system designed to manage weather data stored in CSV files. The architecture of the system consists of a manager, multiple keepers, and their replicas, along with a command-line client for user interaction. The system uses a consistent hash ring for data distribution and has automatic recovery mechanisms for node failures.

## Architecture

The system is composed of the following components:
- **Manager**: Coordinates the overall operation of the system and manages the consistent hash ring.
- **Keepers**: Store portions of the data array and replicate their data to their respective replicas.
- **Replicas**: Maintain copies of the data stored by their associated keepers.
- **Client**: A command-line interface for interacting with the system.

### Components

#### Manager
- Initializes and maintains the consistent hash ring
- Coordinates data distribution using consistent hashing
- Processes client requests (LOAD and GET)
- Manages communication between components using RabbitMQ
- Maintains a data index for quick lookups
- Performs regular health checks on keepers and handles failure recovery

#### Keepers
- Store actual data in memory
- Each keeper is responsible for its portion of the data as determined by the consistent hash ring
- Automatically replicate data to their associated replica
- Handle data storage and retrieval requests
- Respond to health check messages from the manager

#### Replicas
- Maintain exact copies of their associated keeper's data
- Provide data redundancy
- Can serve read requests when needed
- Support data migration in case of keeper failures

#### Client
- Provides command-line interface
- Supports multiple date formats (DD-MM-YYYY, YYYY-MM-DD, YYYYMMDD)
- Handles connection management and error recovery

### Consistent Hashing Implementation

The system uses a consistent hashing ring to distribute data across keepers. This is implemented in the `ConsistentHashRing` class:

```python
class ConsistentHashRing:
    def __init__(self):
        self.ring = []        # Stores hash values
        self.nodes = {}       # Maps node names to hash values

    def add_node(self, node):
        # Add a new node to the hash ring
        self.nodes[node] = self._hash(node)
        self.ring.append(self.nodes[node])
        self.ring.sort()

    def remove_node(self, node):
        # Remove a node from the hash ring
        if node in self.nodes:
            self.ring.remove(self.nodes[node])
            del self.nodes[node]

    def get_node(self, key):
        # Find the node responsible for a given key
        if not self.ring:
            return None
        key_hash = self._hash(key)
        idx = bisect.bisect(self.ring, key_hash) % len(self.ring)
        return list(self.nodes.keys())[idx]
```

The consistent hash ring is used in two main operations:
1. During LOAD: To determine which keeper should store each date's data
2. During GET: To locate the keeper that holds the requested date's data

## System Workflow

### Initialization Process
1. **Manager Startup**:
   - Manager initializes the RabbitMQ connection and creates necessary queues
   - Creates a consistent hash ring for data distribution
   - Starts the specified number of keeper processes
   - Initializes the health check monitoring thread

2. **Keeper Startup**:
   - Each keeper initializes its RabbitMQ connection
   - Creates its own message queue
   - Starts its associated replica process
   - Begins listening for messages from the manager

3. **Replica Startup**:
   - Each replica initializes its RabbitMQ connection
   - Creates its message queue
   - Begins listening for replication messages from its associated keeper

### Data Flow

#### Load Operation
1. **Client Request**:
   - Client sends LOAD command with filename to the manager
   - Manager acknowledges the request and begins processing

2. **File Processing**:
   - Manager reads and parses the CSV file
   - Identifies the date column in the data
   - Standardizes date formats for consistent processing
   - Groups records by date

3. **Data Distribution**:
   - For each unique date in the data:
     - Manager uses consistent hash ring to determine target keeper
     - Manager sends data to the assigned keeper
     - Keeper stores data in its memory
     - Keeper replicates data to its associated replica
     - Manager updates its data index to track which keeper stores which date

4. **Client Notification**:
   - Manager sends progress updates to the client during processing
   - When complete, manager sends a final success message to the client

#### Get Operation
1. **Client Request**:
   - Client sends GET command with date to the manager
   - Manager standardizes the date format

2. **Data Retrieval**:
   - Manager checks its data index to find which keeper(s) store the requested date
   - If found in index, manager queries the indexed keeper(s)
   - If not found in index or indexed keepers don't have the data:
     - Manager uses hash ring to identify potential keepers
     - Manager tries up to three other keepers to find the data

3. **Response Processing**:
   - If data is found, manager formats the response and sends it to the client
   - If no data is found, manager sends an appropriate error message
   - Manager updates its data index if data was found in a non-indexed keeper

### Fault Tolerance and Recovery

The system implements a comprehensive fault tolerance mechanism to handle keeper failures:

#### Health Monitoring
1. **Regular Health Checks**:
   - Manager runs a dedicated health check thread
   - Every 8 seconds, manager sends health check messages to all keepers
   - Keepers respond to confirm they are alive
   - Manager also runs a system status check every 7 seconds to report overall system health

2. **Failure Detection**:
   - If a keeper fails to respond to a health check, it is marked as dead
   - Manager initiates the failure recovery process in a separate thread
   - This ensures that health checks continue while recovery is in progress

#### Failure Recovery Process
1. **Preparation**:
   - Manager marks the failed keeper as unavailable in its status tracking
   - Checks if there are other available keepers to handle redistribution
   - Verifies that the replica of the failed keeper is accessible

2. **Target Selection**:
   - Manager selects the next keeper in the hash ring to take over the failed keeper's data
   - Verifies that the selected keeper is healthy and available
   - If not, tries to find another available keeper

3. **Data Migration**:
   - Manager initiates data migration from the failed keeper's replica to the target keeper
   - The migration process is now implemented with a batch approach to handle large datasets:
     - First, manager retrieves a list of all dates from the replica
     - Then, it retrieves data in batches (50 dates per batch) to avoid RabbitMQ message size limits
     - For each batch, data is sent to the target keeper with the original data structure preserved

4. **System Update**:
   - If migration is successful, manager updates the data index to reflect the new data locations
   - If migration fails, manager removes the failed keeper from the data index
   - Manager removes the failed keeper from the hash ring
   - Manager terminates the failed keeper process
   - Manager terminates the replica after successful data migration

5. **Recovery Completion**:
   - System continues to operate with the remaining keepers
   - Client requests are automatically routed to the appropriate keepers

#### Batch Data Migration Implementation

To handle large datasets that exceed RabbitMQ's message size limit (16MB), we implemented a batch-based migration approach:

1. **Date List Retrieval**:
   - Manager sends a `GET_DATE_LIST` request to the replica
   - Replica responds with a list of all dates it has stored
   - This avoids sending all data at once, which could exceed message size limits

2. **Batch Processing**:
   - Manager processes dates in batches of 50
   - For each batch, manager sends a `GET_DATA_BATCH` request to the replica
   - Replica responds with data only for the requested dates
   - Manager sends this data to the target keeper

3. **Error Handling**:
   - Each step includes retry mechanisms (up to 3 attempts)
   - Detailed error logging helps diagnose issues
   - If a batch fails, the system continues with the next batch

This approach ensures that even very large datasets can be migrated successfully without hitting RabbitMQ's message size limits.

## Implementation Details with Code Examples

### Health Check and PING Mechanism

The health check mechanism is implemented in the `check_keeper_health` method of the Manager class:

```python
def check_keeper_health(self, keeper_id):
    """Check if a keeper is healthy by sending a ping message
    Returns True if keeper is healthy, False if not healthy"""
    # If keeper is already marked as unavailable, return False immediately
    if keeper_id in self.keeper_status and not self.keeper_status[keeper_id]:
        return False
        
    try:
        # Create temporary connection and channel
        temp_connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost',
            connection_attempts=3,
            retry_delay=1,
            socket_timeout=5
        ))
        temp_channel = temp_connection.channel()
        
        # Create temporary callback queue
        result = temp_channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue
        
        # Set response flag
        response_received = False
        
        # Define callback function
        def on_response(ch, method, props, body):
            nonlocal response_received
            response_received = True
            print(f"Received health check response from keeper {keeper_id}")
        
        # Set up consumer
        temp_channel.basic_consume(
            queue=callback_queue,
            on_message_callback=on_response,
            auto_ack=True
        )
        
        # Send ping message
        temp_channel.basic_publish(
            exchange='',
            routing_key=f'keeper_{keeper_id}',
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=str(uuid.uuid4())
            ),
            body=json.dumps({
                'type': 'PING',
                'timestamp': time.time()
            })
        )
        
        # Wait for response, maximum 5 seconds
        start_time = time.time()
        while time.time() - start_time < 5:
            temp_connection.process_data_events(time_limit=0.1)
            if response_received:
                break
        
        # Clean up resources and return health status
        try:
            temp_channel.queue_delete(queue=callback_queue)
            temp_connection.close()
        except Exception:
            pass
            
        return response_received
            
    except Exception as e:
        print(f"Error checking health of keeper {keeper_id}: {e}")
        return False
```

This method works by:
1. Creating a temporary connection and queue
2. Sending a PING message to the specified keeper
3. Waiting for a response for up to 5 seconds
4. If a response is received, the keeper is considered healthy; otherwise, it's considered unhealthy

### Node Failure Detection and Consistent Hash Ring Updates

The system periodically checks all keepers' health status through the `check_keepers_health` method:

```python
def check_keepers_health(self):
    """Check if all keepers are responsive"""
    for i in range(self.num_keepers):
        # Skip keepers already known to be dead
        if i in self.keeper_status and not self.keeper_status[i]:
            continue
            
        # Skip keepers that have been removed
        if self.keepers[i] is None:
            continue
            
        # Send health check message
        try:
            # Use separate connection for health check
            temp_connection = None
            
            try:
                # ... connection and channel creation ...
                
                # If no response, mark as dead and handle failure
                if not response_received:
                    print(f"Keeper {i} is not responding, marking as dead")
                    self.keeper_status[i] = False
                    
                    # Handle failure in a separate thread to avoid blocking health checks
                    failure_thread = threading.Thread(
                        target=self.handle_keeper_failure,
                        args=(i,)
                    )
                    failure_thread.daemon = True
                    failure_thread.start()
                else:
                    # Keeper is alive
                    self.keeper_status[i] = True
            
            except Exception as e:
                print(f"Error during health check for keeper {i}: {e}")
                # ... error handling ...
                
        except Exception as e:
            print(f"Critical error checking health of keeper {i}: {str(e)}")
```

When a node failure is detected, the system updates the consistent hash ring in the `handle_keeper_failure` method:

```python
def handle_keeper_failure(self, failed_keeper_id):
    """Handle the failure of a keeper by redistributing its data"""
    print(f"Handling failure of keeper {failed_keeper_id}")
    
    try:
        # ... prevent duplicate handling of the same failure ...
        
        # Mark keeper as failed
        self.keeper_status[failed_keeper_id] = False
        
        # ... check if there are available keepers ...
        
        # ... ensure replica queue exists ...
        
        # Select next keeper to take over
        # ... selection logic ...
        
        # Migrate data
        migration_success = self.migrate_data_from_replica(failed_keeper_id, next_keeper_id)
        
        # Only update data index if migration was successful
        if migration_success:
            self.update_data_index(failed_keeper_id, next_keeper_id)
        else:
            # If migration failed, just remove the failed keeper from the data index
            self.remove_from_data_index(failed_keeper_id)
        
        # Remove failed keeper from the hash ring
        self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')
        print(f"Removed keeper {failed_keeper_id} from the hash ring")
        
        # Terminate keeper process
        if self.keepers[failed_keeper_id]:
            self.keepers[failed_keeper_id].terminate()
            self.keepers[failed_keeper_id] = None
        
        # ... cleanup and logging ...
    
    except Exception as e:
        print(f"Error handling keeper failure: {e}")
        # ... ensure cleanup of processing flag ...
```

The consistent hash ring is updated by removing the failed node with `self.hash_ring.remove_node(f'keeper_{failed_keeper_id}')`, which redistributes the data to the remaining nodes.

### Data Migration from Replica

When a keeper fails, the system migrates data from its replica to a new keeper using the batch migration approach:

```python
def migrate_data_from_replica(self, source_keeper_id, target_keeper_id):
    """Migrate data from a replica to another keeper"""
    print(f"Migrating data from replica of keeper {source_keeper_id} to keeper {target_keeper_id}")
    
    # Check if target keeper is healthy
    if not self.check_keeper_health(target_keeper_id):
        print(f"Target keeper {target_keeper_id} is not healthy, cannot migrate data")
        return False
    else:
        print(f"Target keeper {target_keeper_id} is healthy, proceeding with migration")
    
    # First get all date list
    date_list = self.get_date_list_from_replica(source_keeper_id)
    if date_list is None:
        print(f"Failed to get date list from replica of keeper {source_keeper_id}")
        return False
    
    if not date_list:
        print(f"No data to migrate from replica of keeper {source_keeper_id} (empty date list)")
        self.terminate_replica(source_keeper_id)
        return True
    
    print(f"Retrieved {len(date_list)} dates from replica of keeper {source_keeper_id}")
    
    # Process data in batches
    batch_size = 50  # Process 50 dates per batch
    total_dates = len(date_list)
    dates_migrated = 0
    success = True
    
    for i in range(0, total_dates, batch_size):
        batch_dates = date_list[i:min(i+batch_size, total_dates)]
        print(f"Processing batch {i//batch_size + 1}/{(total_dates+batch_size-1)//batch_size}, {len(batch_dates)} dates")
        
        # Get data for this batch of dates
        batch_data = self.get_data_batch_from_replica(source_keeper_id, batch_dates)
        if batch_data is None:
            print(f"Failed to get data for batch {i//batch_size + 1}")
            success = False
            continue
        
        # Send data to target keeper
        for date, date_data in batch_data.items():
            try:
                # ... data processing and sending ...
                
                # Send complete data structure to target keeper
                self.send_to_keeper(target_keeper_id, create_message('STORE', {
                    'date': date,
                    'data': date_data,
                    'source_file': 'migration'
                }))
                
                dates_migrated += 1
                
            except Exception as e:
                print(f"Error sending data for date {date}: {e}")
                success = False
    
    print(f"Data migration completed: {dates_migrated}/{total_dates} dates migrated to keeper {target_keeper_id}")
    
    # Terminate replica
    self.terminate_replica(source_keeper_id)
    
    return success and dates_migrated > 0
```

The replica is terminated after successful migration using the `terminate_replica` method:

```python
def terminate_replica(self, keeper_id):
    """Terminate the replica of a keeper"""
    print(f"Terminating replica of keeper {keeper_id}")
    
    # Add retry mechanism
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        # ... retry logic ...
        
        try:
            # Create new connection and channel
            temp_connection = None
            
            try:
                # ... connection and channel creation ...
                
                # Check if replica queue exists
                replica_queue = f'replica_{keeper_id}'
                # ... check logic ...
                
                # Send termination message to replica
                temp_channel.basic_publish(
                    exchange='',
                    routing_key=f'replica_{keeper_id}',
                    body=create_message('TERMINATE', {})
                )
                print(f"Sent termination message to replica of keeper {keeper_id}")
                
                # ... wait and confirm termination ...
                
                return True
                
            except Exception as e:
                # ... error handling ...
                
        except Exception as e:
            # ... error handling ...
    
    print(f"Failed to terminate replica of keeper {keeper_id} after {max_retries} attempts")
    return True  # Even if termination fails, return True as this should not block the entire failure handling process
```

On the replica side, the `handle_terminate` method processes the termination request:

```python
def handle_terminate(self):
    """Handle termination request from manager"""
    self.log(f"Replica {self.keeper_id} received termination request", True)
    
    try:
        # Clean up resources
        if self.connection:
            self.connection.close()
            
        self.log(f"Replica {self.keeper_id} is shutting down", True)
        # Exit the process
        sys.exit(0)
    except Exception as e:
        self.log(f"Error during replica termination: {str(e)}", True)
        traceback.print_exc()
        sys.exit(1)
```

### Data Replication from Keeper to Replica

When a keeper receives a `STORE` message, it stores the data and replicates it to its replica:

```python
def handle_store(self, data):
    date = data.get('date')
    records = data.get('data')
    column_names = data.get('column_names', [])
    source_file = data.get('source_file', 'unknown')
    
    # ... data storage logic ...
    
    # Replicate to replica
    self.replicate_to_replica(date, {
        'date': date,
        'data': records,
        'column_names': column_names,
        'source_file': source_file
    })
    
    return {'success': True}
```

The replication to replica is implemented in the `replicate_to_replica` method:

```python
def replicate_to_replica(self, date, data):
    """Replicate data to the replica"""
    try:
        self.channel.basic_publish(
            exchange='',
            routing_key=f'replica_{self.keeper_id}',
            body=create_message('REPLICATE', data)
        )
        self.log(f"Replicated data for date {date} to replica")
    except Exception as e:
        self.log(f"Error replicating data to replica: {e}", True)
        traceback.print_exc()
```

On the replica side, the `handle_replicate` method processes the replication request:

```python
def handle_replicate(self, data):
    try:
        date = data.get('date')
        source_file = data.get('source_file', 'unknown')
        
        # Check if this is migrated data with complete structure
        if source_file == 'migration' and isinstance(data.get('data'), dict):
            self.log(f"Received migrated data with complete structure for date {date}")
            
            # Use the complete data structure from migration directly
            if 'datasets' in data.get('data'):
                self.log(f"Using complete datasets structure with {len(data.get('data')['datasets'])} datasets")
                self.data[date] = data.get('data')
            else:
                # ... handle other data formats ...
            
            self.log(f"Replica storage complete, date: {date}, using complete structure")
            return {'success': True}
        
        # Check data structure
        if isinstance(data.get('data'), dict) and 'datasets' in data.get('data'):
            # New format (with datasets field)
            datasets = data.get('data').get('datasets', [])
            self.log(f"Replicating data with datasets structure for date {date}")
            
            # Use complete data structure
            self.data[date] = data.get('data')
            self.log(f"Replica storage complete, date: {date}, with {len(datasets)} datasets")
        else:
            # Old format
            records = data.get('data', [])
            column_names = data.get('column_names', [])
            
            self.log(f"Replicating {len(records)} records for date {date}")
            
            self.data[date] = {
                'data': records,
                'column_names': column_names,
                'source_file': source_file
            }
            
            self.log(f"Replica storage complete, date: {date}, record count: {len(records)}")
        
        return {'success': True}
    except Exception as e:
        self.log(f"Error processing REPLICATE request: {e}", True)
        traceback.print_exc()
        return {'success': False, 'error': str(e)}
```

## Data Structure

The system uses a consistent data structure across all components:

1. **Manager's Data Index**:
   - Maps dates to lists of keeper IDs that store data for those dates
   - Enables quick lookups during GET operations

2. **Keeper's Data Storage**:
   - Organizes data by date
   - For each date, stores:
     - `datasets`: Array of dataset objects
     - Each dataset contains:
       - `data`: The actual records
       - `column_names`: Column headers from the CSV
       - `source_file`: Origin of the data

3. **Replica's Data Storage**:
   - Mirrors the structure of its associated keeper
   - Maintains the complete data structure to ensure data integrity during migration

## Message Types
The system uses various message types for different operations:

1. **Data Operations**:
   - `STORE`: Send data to a keeper for storage
   - `GET`: Request data for a specific date
   - `REPLICATE`: Replicate data from keeper to replica

2. **Health Monitoring**:
   - `HEALTH_CHECK`: Check if a keeper is alive
   - `PING`: Simplified health check

3. **Failure Recovery**:
   - `GET_DATE_LIST`: Get list of all dates from a replica
   - `GET_DATA_BATCH`: Get data for a batch of dates
   - `GET_ALL_DATA`: Legacy method to get all data at once (replaced by batch approach)
   - `TERMINATE`: Signal a replica to shut down

### Error Handling
- Connection recovery mechanisms
- Request timeouts with configurable durations
- Retry mechanisms for critical operations
- Detailed error logging
- Graceful degradation when components fail

## Future Enhancements

The current implementation provides a foundation for several advanced features:
1. Dynamic scaling with minimal data redistribution
2. Advanced replication strategies
3. Data persistence and recovery
4. Load balancing and monitoring
5. Support for even larger datasets through improved batching and streaming

## Requirements

- Python 3.x
- RabbitMQ server

## Usage

1. Start the RabbitMQ server.
2. Run the manager:
   ```bash
   python manager.py 3
   ```
   This will start the manager with 3 keepers.
   
3. Use the client to load data and retrieve records:
   ```bash
   python client.py
   ```

### Available Commands
- `LOAD [filename]`: Load and distribute data from a CSV file
- `GET [date]`: Retrieve records for a specific date
- `exit` or `quit`: Exit the client

