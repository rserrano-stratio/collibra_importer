from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Boolean

Base = declarative_base()


class CollibraImportProcess(Base):
    __tablename__ = "collibra_import_process"

    id = Column(Integer, primary_key=True, autoincrement=True)
    records_count = Column(Integer)
    import_date = Column(DateTime(timezone=True), server_default=func.now())

class CollibraDBRecord(Base):
    __tablename__ = "collibra_db_record"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dd_l0 = Column(String)
    dd_l1 = Column(String)
    dd_l2 = Column(String)
    dd_l3 = Column(String)
    de_name = Column(String)
    dc_requirement_description = Column(String)
    req_dimension_id = Column(String)
    dqr_target = Column(Float)
    qr_generic_name = Column(String)
    import_process_id = Column(Integer)
    stratio_qr_dimension = Column(String)
    metadatapath = Column(String)

class CollibraStatus(Base):
    __tablename__ = "collibra_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    metadatapath = Column(String)
    stratio_qr_dimension = Column(String)
    stratio_qr_name = Column(String)
    stratio_qr_id = Column(Integer)
    stratio_qr_status = Column(String)
    stratio_qr_active = Column(Boolean)
    qr_threshold = Column(Float)
    creation_date = Column(DateTime(timezone=True), server_default=func.now())
    update_date = CColumn(DateTime(timezone=True), onupdate=func.now())
    import_process_id = Column(Integer)

