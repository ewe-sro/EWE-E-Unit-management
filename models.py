from sqlalchemy import Column, String, Integer, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Controller(Base):
    __tablename__ = "charging_controllers"

    device_uid = Column(String, primary_key=True)
    charging_point_name = Column(String)
    states = relationship("State", backref="controller", lazy=True)
    states = relationship("Session", backref="controller", lazy=True)
    states = relationship("Email", backref="controller", lazy=True)

class State(Base):
    __tablename__ = "last_known_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state = Column(String)
    device_uid = Column(Integer, ForeignKey("charging_controllers.device_uid"), nullable=False)

class Session(Base):
    __tablename__ = "charging_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    start_real_power_Wh = Column(Integer)
    end_real_power_Wh = Column(Integer)
    consumption_Wh = Column(Integer)
    start_timestamp = Column(DateTime)
    end_timestamp = Column(DateTime)
    device_uid = Column(Integer, ForeignKey("charging_controllers.device_uid"), nullable=False)

class Email(Base):
    __tablename__ = "sent_emails"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    sender = Column(String)
    receiver = Column(String)
    device_uid = Column(Integer, ForeignKey("charging_controllers.device_uid"), nullable=False)