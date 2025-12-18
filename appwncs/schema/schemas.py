from pydantic import BaseModel, Field,field_validator

class WncsRequest(BaseModel):
    rowfile_name: str
    cell_status_name: str

    @field_validator("rowfile_name")
    @classmethod
    def zip_validation(cls, v):
        if not v.lower().endswith(".zip"):
            raise ValueError("rowfile file should be zip ")
        return v

    @field_validator("cell_status_name")
    @classmethod
    def excel_validation(cls, v):
        if not v.lower().endswith((".xlsx", ".xls")):
            raise ValueError("cell_status file should be Excel")
        return v




