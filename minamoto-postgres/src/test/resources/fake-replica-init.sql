-- Make this instance appear as a replica by creating a schema-shadowing function.
-- The driver queries: SELECT NOT pg_is_in_recovery()
-- We override search_path at the database level so public.pg_is_in_recovery() is found first.
CREATE OR REPLACE FUNCTION pg_is_in_recovery() RETURNS bool LANGUAGE sql AS $$ SELECT true $$;
ALTER DATABASE testdb SET search_path = public, pg_catalog;
