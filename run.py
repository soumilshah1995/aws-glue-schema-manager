import boto3
from botocore.exceptions import ClientError

class GlueSchemaManager:
    def __init__(self, registry_name, schema_name, data_format, schema_definition):
        """
        Initializes the GlueSchemaManager with necessary parameters.

        :param registry_name: The name of the Glue registry.
        :param schema_name: The name of the Glue schema.
        :param data_format: The data format (e.g., AVRO, JSON).
        :param schema_definition: The schema definition (e.g., AVRO schema).
        """
        self.registry_name = registry_name
        self.schema_name = schema_name
        self.data_format = data_format
        self.schema_definition = schema_definition
        self.glue_client = boto3.client('glue')

    def create_registry_if_not_exists(self):
        """
        Creates a Glue registry if it does not already exist.
        """
        try:
            self.glue_client.create_registry(RegistryName=self.registry_name)
            print(f"Registry '{self.registry_name}' created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(f"Registry '{self.registry_name}' already exists.")
            else:
                raise

    def create_schema_if_not_exists(self):
        """
        Creates a Glue schema within the registry if it does not already exist.
        """
        try:
            self.glue_client.create_schema(
                RegistryId={'RegistryName': self.registry_name},
                SchemaName=self.schema_name,
                DataFormat=self.data_format,
                Compatibility='FORWARD',
                SchemaDefinition=self.schema_definition
            )
            print(f"Schema '{self.schema_name}' created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(f"Schema '{self.schema_name}' already exists.")
            else:
                raise

    def add_schema_version(self):
        """
        Registers a new version of the schema in the Glue registry.
        """
        try:
            response = self.glue_client.register_schema_version(
                SchemaId={
                    'SchemaName': self.schema_name,
                    'RegistryName': self.registry_name
                },
                SchemaDefinition=self.schema_definition
            )
            version = response['VersionNumber']
            print(f"Schema version added successfully. Version: {version}")
            return version
        except ClientError as e:
            print(f"Error adding schema version: {e}")
            return None

    def read_schema(self):
        """
        Retrieves the latest schema version from the Glue registry.
        """
        try:
            response = self.glue_client.get_schema_version(
                SchemaId={
                    'SchemaName': self.schema_name,
                    'RegistryName': self.registry_name
                },
                SchemaVersionNumber={'LatestVersion': True}
            )
            print(f"Retrieved schema '{self.schema_name}':")
            print(response['SchemaDefinition'])
        except ClientError as e:
            print(f"Error reading schema: {e}")

    def update_schema_version_metadata(self, version_number, metadata):
        """
        Updates the metadata of a specific schema version.
        """
        try:
            self.glue_client.put_schema_version_metadata(
                SchemaId={
                    'SchemaName': self.schema_name,
                    'RegistryName': self.registry_name
                },
                SchemaVersionNumber={'VersionNumber': version_number},
                MetadataKeyValue=metadata
            )
            print(f"Updated metadata for schema version {version_number}")
        except ClientError as e:
            print(f"Error updating schema version metadata: {e}")

    def set_schema_version_checkpoint(self, version_number):
        """
        Sets a specific schema version as the checkpoint.
        """
        try:
            self.glue_client.put_schema_version_metadata(
                SchemaId={
                    'SchemaName': self.schema_name,
                    'RegistryName': self.registry_name
                },
                SchemaVersionNumber={'VersionNumber': version_number},
                MetadataKeyValue={
                    'MetadataKey': 'Checkpoint',
                    'MetadataValue': 'true'
                }
            )
            print(f"Set schema version {version_number} as checkpoint")
        except ClientError as e:
            print(f"Error setting schema version checkpoint: {e}")

def main():
    """
    Main function to execute the Glue schema management operations.
    """
    # Configuration
    registry_name = 'my-registry'
    schema_name = 'my-avro-schema'
    data_format = 'AVRO'
    avro_schema = '''{
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "address", "type": ["null", "string"], "default": null},
        {"name": "age", "type": "int"}
      ]
    }'''

    # Initialize the GlueSchemaManager with the provided configuration
    glue_schema_manager = GlueSchemaManager(registry_name, schema_name, data_format, avro_schema)

    # Create the registry if it doesn't exist
    glue_schema_manager.create_registry_if_not_exists()

    # Create the schema if it doesn't exist
    glue_schema_manager.create_schema_if_not_exists()

    # Add a schema version and capture the version number
    version = glue_schema_manager.add_schema_version()

    # Read and print the latest schema
    glue_schema_manager.read_schema()

    # Set the latest version as checkpoint
    if version:
        glue_schema_manager.set_schema_version_checkpoint(version)

if __name__ == "__main__":
    main()
