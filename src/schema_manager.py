__file__ = "schema_manager.py"
__author__ = "Benji Bui"
__date__ = "2025/07"
__email__ =  "bui.vquang02@gmail.com"
__version__ = "0.0.1"

class SchemaManager:
    SUPPORTED_TYPES = {
        "id", "text", "email", "datetime",
        "categorical", "numerical", "boolean", "integer"
    }
    
    def __init__(self, schema: dict):
        self.schema = schema
        self.validate_schema()
        
    def validate_schema(self):
        if "columns" not in self.schema:
            raise ValueError("Schema must include a 'columns' key.")
        
        for col, props in self.schema["columns"].items():
            # col - attribute name
            # props - attribute type
            if "sdtype" not in props:
                raise ValueError(f"Missing 'sdtype' for columns: {col}")
            
            sdtype = props["sdtype"]
            if sdtype not in self.SUPPORTED_TYPES:
                raise ValueError(f"Unsupported sdtype '{sdtype}' for columns: {col}")