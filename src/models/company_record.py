from dataclasses import dataclass, field, fields as dataclass_fields
from typing import List, Dict, Optional, Set

@dataclass
class CompanyRecord:
    url: str
    company_name: Optional[str] = None
    phone_numbers: Set[str] = field(default_factory=set)
    social_media_links: Set[str] = field(default_factory=set)
    addresses: Set[str] = field(default_factory=set)

    def to_dict(self) -> Dict:
        """Converts the dataclass to a dictionary, making sets JSON-serializable."""
        data = {}
        for f in dataclass_fields(self):
            value = getattr(self, f.name)
            # Convert sets to lists for JSON serialization
            if isinstance(value, set):
                data[f.name] = list(value)
            else:
                data[f.name] = value
        return data

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [f.name for f in dataclass_fields(cls)]