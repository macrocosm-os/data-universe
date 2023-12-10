from scraping.config import model
from scraping import coordinator

class ConfigReader:
    """A class to read the scraping config from a json file."""
    
    @classmethod
    def load_config(cls, filepath: str) -> coordinator.CoordinatorConfig:
        """Loads the scraping config from json and returns it as a CoordinatorConfig.
        
        Raises:
            ValidationError: if the file content is not valid.
        """
        
        print(f"Loading file: {filepath}")
        parsed_file = model.ScrapingConfig.parse_file(path=filepath)
        print(f"Got parsed file: {parsed_file}")
        return parsed_file.to_coordinator_config()