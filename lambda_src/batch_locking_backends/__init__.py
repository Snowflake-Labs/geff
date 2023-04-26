'''
These backends help lock batches so that they are not processed twice when the call driver takes >30s
and the API Gateway times out and Snowflake retries the request. 

Each batch has a lock that goes through three states:

1. Un-initialized batch has not yet been seen by GEFF and so a lock does not exist
2. Initialized batch is "processing" and has a "locked" lock while the call driver is running
3. Finished batch means a call driver has responded or timed out and the lock is "unlocked" and stores the response

The modules should expose:

is_batch_initialized(batch_id) to return false for state (1) and true otherwise
initialize_batch(batch_id) to move a lock from (1) to (2)
is_batch_processing(batch_id) to return true for state (2) and false otherwise
finish_batch_processing(batch_id, response) to move lock to state (3) and store a response value
get_response_for_batch(batch_id) to return the response value for the batch on stage (3) and None otherwise
after a batch is finalized, locks should exist for at least 24h to allow for debugging
'''
