-- Step 4: Drop the throwaway table (separate step to allow sync of create first)
-- Run after a delay to ensure the create has synced

DROP TABLE integ_test_db.to_be_dropped;
