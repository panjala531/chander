import argparse, sys
import deployment.dbt_logger as dbt_logger
import deployment.snowflake_handler as sf_handler

log = dbt_logger.initialize(__name__)

sf_connection = None
sf_cursor = None
command = None
host_name = None

def parse_args(argv=None):
    """
    Parse command-line arguments to set global variables.
    """
    global command, host_name
    parser = argparse.ArgumentParser()
    parser.add_argument('--command', help='Command to issue to the host (e.g., "shutdown")', type=str)
    parser.add_argument('--host_name', help='Name of the host to which the command will be issued', type=str)
    args = parser.parse_args(argv)
    command = args.command
    host_name = args.host_name

def ensure_host_command_column():
    """
    Ensures the HOST_COMMAND column exists in the DBT_HOSTS table.
    """
    global sf_cursor
    try:
        if sf_cursor is None:
            sf_cursor = sf_handler.get_sf_cursor()

        # Check if the column exists
        check_column_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'ARIP.OPS'
              AND table_name = 'DBT_HOSTS'
              AND column_name = 'HOST_COMMAND'
        """
        sf_cursor.execute(check_column_query)
        column_exists = sf_cursor.fetchone()

        # Add the column if it doesn't exist
        if not column_exists:
            log.info("HOST_COMMAND column does not exist. Adding it to ARIP.OPS.DBT_HOSTS table.")
            add_column_query = """
                ALTER TABLE ARIP.OPS.DBT_HOSTS
                ADD COLUMN HOST_COMMAND STRING
            """
            sf_cursor.execute(add_column_query)
            log.info("Successfully added HOST_COMMAND column to ARIP.OPS.DBT_HOSTS table.")
    except Exception as e:
        log.exception("Error ensuring HOST_COMMAND column exists - Exception: %s", str(e))
        raise e

def issue_host_command(command, host_name):
    """
    Updates the HOST_COMMAND column in the DBT_HOSTS table for a specified host.
    """
    global sf_cursor, sf_connection
    try:
        if sf_cursor is None:
            sf_cursor = sf_handler.get_sf_cursor()

        if not command or not host_name:
            log.error("Command or host name not provided.")
            return

        # Ensure the HOST_COMMAND column exists
        ensure_host_command_column()

        # Update the column with the specified command
        query = f"""
            UPDATE ARIP.OPS.DBT_HOSTS
            SET HOST_COMMAND = '{command}'
            WHERE HOST_NAME = '{host_name}'
        """
        log.info("Executing host command update: %s", query)
        sf_cursor.execute(query)
        sf_connection.commit()
        log.info("Successfully issued command '%s' to host '%s'", command, host_name)
    except Exception as e:
        log.exception("Error in issuing host command - Exception: %s", str(e))
        raise e

def main():
    """
    Main function to process command-line arguments and issue the specified host command.
    """
    log.info("Starting script to issue commands to hosts.")
    parse_args(argv=sys.argv[1:])
    if command and host_name:
        issue_host_command(command=command, host_name=host_name)
    else:
        log.info("No command or host name specified. Skipping command issuance.")

if __name__ == "__main__":
    log.info("Initializing Host Command Issuance Script")
    main()
